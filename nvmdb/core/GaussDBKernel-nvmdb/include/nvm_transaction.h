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
 * nvm_transaction.h
 *
 * IDENTIFICATION
 *   src/gausskernel/storage/nvmdb/core/GaussDBKernel-nvmdb/include/nvm_transaction.h
 * -------------------------------------------------------------------------
 */
#ifndef NVMDB_TRANSACTION_H
#define NVMDB_TRANSACTION_H

#include <vector>

#include "nvm_undo_ptr.h"
#include "nvm_undo_segment.h"
#include "nvm_undo_context.h"
#include "nvm_undo_page.h"
#include "nvm_tuple.h"
#include "nvm_rowid_map.h"

namespace NVMDB {

enum TransactionStatus {
    TX_EMPTY,
    TX_IN_PROGRESS,
    TX_WAIT_ABORT,
    TX_COMMITTING,
    TX_ABORTED,
    TX_COMMITTED,
};

enum TM_Result {
    /*
     * Signals that the action succeeded (i.e. update/delete performed, lock was acquired)
     */
    TM_Ok,

    /* The affected tuple wasn't visible to the relevant snapshot */
    TM_Invisible,

    /* The affected tuple was already modified by the calling backend */
    TM_SelfModified,

    /* The affected tuple was updated by another transaction. */
    TM_Updated,

    /* The affected tuple was deleted by another transaction */
    TM_Deleted,

    /*
     * The affected tuple is currently being modified by another session. This will only be returned
     * if (update/delete/lock)_tuple are instructed not to wait.
     */
    TM_BeingModified,
    TM_SelfCreated,
    TM_SelfUpdated,

    /* The transaction generated this version is aborted. */
    TM_Aborted,
};

class Transaction {
public:
    char *undoRecordCache;
    Transaction();
    ~Transaction();

    void PrepareUndo();
    void Begin();
    void Commit();
    void Abort();
    void WaitAbort()
    {
        tx_status = TX_WAIT_ABORT;
    }

    TransactionStatus GetTrxStatus()
    {
        return tx_status;
    }

    TransactionSlotPtr GetTrxSlotLocation()
    {
        return trx_slot_ptr;
    }

    uint64 GetCSN()
    {
        return csn;
    }

    uint64 GetSnapshot()
    {
        return snapshot;
    }

    uint64 GetMinSnapshot()
    {
        return min_snapshot;
    }

    UndoRecPtr InsertUndoRecord(UndoRecord *record)
    {
        return undo_trx->InsertUndoRecord(record);
    }

    TM_Result VersionIsVisible(NVMTuple *tuple);
    TM_Result SatisifiedUpdate(NVMTuple *tuple);

    void PushWriteSet(RowIdMapEntry *row)
    {
        write_set.push_back(row);
    }
private:
    constexpr static uint32 INVALID_PROC_ARRAY_INDEX = 0xffffffff;
    uint32 local_proc_array_idx{INVALID_PROC_ARRAY_INDEX};
    TransactionSlotPtr trx_slot_ptr;
    UndoTrxContext *undo_trx;
    uint64 snapshot;
    uint64 csn;
    uint64 min_snapshot;  // 后台线程检测出来的所以事务中最小的 snapshot，
    TransactionStatus tx_status;
    std::vector<RowIdMapEntry *> write_set;

    void InstallSnapshot();
    void UninstallSnapshot();
    void ProcArrayAdd();
    void ProcArrayRemove();
};

Transaction *GetCurrentTrxContext();

uint64 GetMinSnapshot();

void RecoveryCSN(const uint64 &max_undo_csn);

void InitGlobalProcArray();
void DestroyGlobalProcArray();

void InitTransactionContext();
void DestroyTransactionContext();

}  // namespace NVMDB

#endif  // NVMDB_TRANSACTION_H