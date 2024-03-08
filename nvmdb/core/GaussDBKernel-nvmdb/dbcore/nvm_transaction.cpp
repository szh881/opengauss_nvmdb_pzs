/*
 * Copyright (c) 2023 Huawei Technologies Co.,Ltd.
 *
 * openGauss is licensed under Mulan PSL v2.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *
 *          http://license.coscl.org.cn/MulanPSL2
 *
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 * -------------------------------------------------------------------------
 *
 * nvm_transaction.cpp
 *
 * IDENTIFICATION
 *   src/gausskernel/storage/nvmdb/core/GaussDBKernel-nvmdb/dbcore/nvm_transaction.cpp
 * -------------------------------------------------------------------------
 */
#include <cstring>

#include "nvm_tuple.h"
#include "nvm_undo_api.h"
#include "nvm_transaction.h"
#include "nvm_cfg.h"

namespace NVMDB {

static std::atomic<uint64> COMMIT_SEQUENCE_NUM{MIN_TRX_CSN};

constexpr int PROC_ARRAY_LOCKED = 0x00000001;
static volatile uint32 ProcArrayLock = 0;

void AcquireProcArrayLock()
{
    do {
        uint32 old_val = ProcArrayLock;
        if (old_val & PROC_ARRAY_LOCKED) {
            continue;
        }
        uint32 new_val = old_val | PROC_ARRAY_LOCKED;
        if (__sync_bool_compare_and_swap(&ProcArrayLock, old_val, new_val)) {
            return;
        }
    } while (true);
}

void ReleaseProcArrayLock()
{
    uint32 old_val = ProcArrayLock;
    Assert(old_val & PROC_ARRAY_LOCKED);
    uint32 new_val = old_val & ~PROC_ARRAY_LOCKED;
    ProcArrayLock = new_val;
}

bool IsValidCsn(uint64 csn)
{
    return csn != 0 && csn >= MIN_TRX_CSN;
}

struct PROC {
    bool inUsed = false;
    std::atomic<uint64> snapshotCsn{MIN_TRX_CSN};
};

static std::atomic<uint64> g_procArrayVersion{0};
static volatile PROC *g_procArray = nullptr;
uint32 g_procArrayIndex = 0;

void InitGlobalProcArray()
{
    Assert(g_procArray == nullptr);
    g_procArray = new PROC[NVMDB_MAX_THREAD_NUM];
}

void DestroyGlobalProcArray()
{
    Assert(g_procArray != nullptr);
    delete[] g_procArray;
    g_procArray = nullptr;
}

static volatile uint64 MIN_SNAPSHOT = MIN_TRX_CSN;
constexpr int SLEEP_TIME = 10000;

uint64 GetMinSnapshot()
{
    uint32 idx = 0;
    uint32 loopCount = 0;
    uint64 minSnapshot = COMMIT_SEQUENCE_NUM;
    uint64 tmpSnapshot = minSnapshot;

    again:
    if (loopCount >= 3) {
        loopCount = 0;
        usleep(SLEEP_TIME);
    }
    uint64 vOld = g_procArrayVersion.load(std::memory_order_acquire);
    for (idx = 0; idx < NVMDB_MAX_THREAD_NUM; idx++) {
        tmpSnapshot = g_procArray[idx].snapshotCsn.load(std::memory_order_relaxed);
        Assert(IsValidCsn(tmpSnapshot));
        if (tmpSnapshot < minSnapshot) {
            minSnapshot = tmpSnapshot;
        }
    }
    std::atomic_thread_fence(std::memory_order_acq_rel);
    uint64 vNew = g_procArrayVersion.load(std::memory_order_relaxed);
    if (vNew != vOld) {
        loopCount++;
        goto again;
    }

    Assert(minSnapshot >= MIN_SNAPSHOT);
    MIN_SNAPSHOT = minSnapshot;
    return minSnapshot;
}

static inline uint64 NvmGetCSN()
{
    return COMMIT_SEQUENCE_NUM;
}

static inline uint64 NvmAdvanceCSN()
{
    return ++COMMIT_SEQUENCE_NUM;
}

void RecoveryCSN(const uint64 &max_undo_csn)
{
    Assert(IsValidCsn(max_undo_csn));
    COMMIT_SEQUENCE_NUM = max_undo_csn + 1;
}

thread_local Transaction *local_trx_context = nullptr;

void InitTransactionContext()
{
    local_trx_context = new Transaction();
}

void DestroyTransactionContext()
{
    delete local_trx_context;
    local_trx_context = NULL;
}

Transaction *GetCurrentTrxContext()
{
    return local_trx_context;
}

Transaction::Transaction(): undo_trx(nullptr), snapshot(0), csn(0), tx_status(TX_EMPTY)
{
    ProcArrayAdd();
    undoRecordCache = new char[MAX_UNDO_RECORD_CACHE_SIZE];
}

Transaction::~Transaction()
{
    ProcArrayRemove();
    delete[] undoRecordCache;
}

void Transaction::InstallSnapshot()
{
    /* Atomic assign my snapshot where others can see it. */
    g_procArrayVersion.fetch_add(1, std::memory_order_relaxed);
    g_procArray[local_proc_array_idx].snapshotCsn.store(COMMIT_SEQUENCE_NUM, std::memory_order_release);
    snapshot = g_procArray[local_proc_array_idx].snapshotCsn;
    min_snapshot = MIN_SNAPSHOT;
    Assert(IsValidCsn(snapshot));
    Assert(snapshot >= MIN_SNAPSHOT);
}

void Transaction::UninstallSnapshot()
{
    /* invalid registered snapshot. */
    Assert(snapshot == g_procArray[local_proc_array_idx].snapshotCsn);
    Assert(snapshot >= MIN_SNAPSHOT);
}

void Transaction::ProcArrayAdd()
{
    uint32 index;
    AcquireProcArrayLock();
    do {
        index = (g_procArrayIndex++) % NVMDB_MAX_THREAD_NUM;
    } while (g_procArray[index].inUsed);
    Assert(!g_procArray[index].inUsed);
    Assert(IsValidCsn(g_procArray[index].snapshotCsn));
    local_proc_array_idx = index;
    g_procArray[index].inUsed = true;
    ReleaseProcArrayLock();
}

void Transaction::ProcArrayRemove()
{
    AcquireProcArrayLock();
    Assert(local_proc_array_idx < NVMDB_MAX_THREAD_NUM);
    Assert(g_procArray[local_proc_array_idx].inUsed);
    g_procArray[local_proc_array_idx].inUsed = false;
    local_proc_array_idx = INVALID_PROC_ARRAY_INDEX;
    ReleaseProcArrayLock();
}

/* no need for read-only txn to prepare undo. */
void Transaction::PrepareUndo()
{
    if (undo_trx == nullptr) {
        undo_trx = AllocUndoContext();
        trx_slot_ptr = undo_trx->GetTrxSlotLocation();
    }
}

void Transaction::Begin()
{
    Assert(tx_status == TX_EMPTY || tx_status == TX_ABORTED || tx_status == TX_COMMITTED);
    Assert(write_set.empty());
    InstallSnapshot();
    tx_status = TX_IN_PROGRESS;
}

void Transaction::Commit()
{
    Assert(tx_status == TX_IN_PROGRESS);
    tx_status = TX_COMMITTING;
    if (undo_trx != nullptr) {
        csn = NvmGetCSN();
        undo_trx->UpdateTrxSlotCSN(csn);
        undo_trx->UpdateTrxSlotStatus(TRX_COMMITTED);
        NvmAdvanceCSN();
        ReleaseTrxUndoContext(undo_trx);
        undo_trx = nullptr;
        write_set.clear();
    }
    tx_status = TX_COMMITTED;
    UninstallSnapshot();
}

void Transaction::Abort()
{
    if (undo_trx != nullptr) {
        /* 事务开始 rollback，此时事务状态仍然是 IN_PROGRESS, 对于已经 rollback 的tuple， */
        undo_trx->RollBack(undoRecordCache);
        /* 事务完成 undo， 此时 heap 上没有undo的数据 */
        undo_trx->UpdateTrxSlotStatus(TRX_ROLLBACKED);
        ReleaseTrxUndoContext(undo_trx);
        undo_trx = nullptr;
        write_set.clear();
    }
    tx_status = TX_ABORTED;
    UninstallSnapshot();
}

TM_Result Transaction::VersionIsVisible(NVMTuple *tuple)
{
    bool committed = false;
    uint64 version_csn = 0;
    if (TrxInfoIsCsn(tuple->m_trxInfo)) {
        committed = true;
        version_csn = tuple->m_trxInfo;
    } else {
        TransactionInfo trx_info;
        bool recycled = !GetTransactionInfo((TransactionSlotPtr)tuple->m_trxInfo, &trx_info);
        if (recycled) {
            /* fill MIN_SNAPSHOT back to trx_info as upper commit CSN. */
            return TM_Ok;
        }
        switch (trx_info.status) {
            case TRX_ROLLBACKED:
            case TRX_ABORTED:
                return TM_Aborted;
            case TRX_COMMITTED:
                committed = true;
                version_csn = trx_info.CSN;
                break;
            case TRX_IN_PROGRESS:
                committed = false;
                break;
            case TRX_EMPTY:
            default:
                Assert(0);
                break;
        }
    }

    if (committed) {
        if (version_csn < snapshot) {
            return TM_Ok;
        } else {
            return TM_Invisible;
        }
    } else {
        if (undo_trx != nullptr && tuple->m_trxInfo == trx_slot_ptr) {
            return TM_SelfUpdated;
        } else {
            return TM_BeingModified;
        }
    }
}

TM_Result Transaction::SatisifiedUpdate(NVMTuple *tuple)
{
    TM_Result result = VersionIsVisible(tuple);
    switch (result) {
        case TM_Ok:
        case TM_Aborted:
        case TM_SelfUpdated:
            return TM_Ok;
        case TM_Invisible:
        case TM_BeingModified:
            return TM_BeingModified;
        default:
            assert(0);
    }
    not_reachable();
}

}  // namespace NVMDB
