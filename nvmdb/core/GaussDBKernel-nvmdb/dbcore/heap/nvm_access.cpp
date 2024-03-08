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
 * nvm_access.cpp
 *
 * IDENTIFICATION
 *   src/gausskernel/storage/nvmdb/core/GaussDBKernel-nvmdb/dbcore/heap/nvm_access.cpp
 * -------------------------------------------------------------------------
 */
#include "nvm_table.h"
#include "nvm_tuple.h"
#include "heap/nvm_heap_undo.h"
#include "nvm_transaction.h"
#include "heap/nvm_access.h"

namespace NVMDB {

static inline bool CheckTrxStatus(Transaction *trx)
{
    return trx->GetTrxStatus() == TX_WAIT_ABORT;
}

RowId HeapUpperRowId(Table *table)
{
    Assert(table->Ready());
    RowIdMap *rowid_map = table->m_rowidMap;
    return rowid_map->GetUpperRowId();
}

RowId HeapInsert(Transaction *trx, Table *table, RAMTuple *tuple)
{
    Assert(table->m_rowLen == tuple->m_rowLen);
    if (CheckTrxStatus(trx)) {
        return HAM_TRANSACTION_WAIT_ABORT;
    }

    trx->PrepareUndo();
    Assert(table->Ready());
    RowIdMap *rowid_map = table->m_rowidMap;

    /* 分配一个RowId，这时候只是内存中的元数据修改了， NVM上的 NVMTUPLE_USED 标志位还没设上 */
    RowId rowid = rowid_map->InsertVersion();
    RowIdMapEntry *row_entry = rowid_map->GetEntry(rowid);
    char *data = row_entry->m_nvmAddr;

    PrepareInsertUndo(trx, table->SegmentHead(), rowid, tuple->payload());

    row_entry->Lock();
    /* Write tuple to NVM; note marking head as used */
    tuple->InitHead(trx->GetTrxSlotLocation(), InvalidUndoRecPtr, NVMTUPLE_USED, 0);
    tuple->Serialize(data, RealTupleSize(table->GetRowLen()));
    row_entry->sync_dram_cache(RealTupleSize(tuple->payload()));
    row_entry->Unlock();

    trx->PushWriteSet(row_entry);
    return rowid;
}

HAM_STATUS HeapRead(Transaction *trx, Table *table, RowId rowid, RAMTuple *tuple)
{
    Assert(table->m_rowLen == tuple->m_rowLen);
    if (CheckTrxStatus(trx)) {
        return HAM_TRANSACTION_WAIT_ABORT;
    }

    RowIdMap *rowid_map = table->m_rowidMap;
    RowIdMapEntry *row_entry = rowid_map->GetEntry(rowid, true);
    if (row_entry == nullptr) {
        return HAM_READ_ROW_NOT_USED;
    }
    HAM_STATUS status;

    row_entry->Lock();
    char *data = row_entry->read_dram_cache(RealTupleSize(tuple->payload()));
    tuple->Deserialize(data);
    if (!tuple->IsInUsed()) {
        status = HAM_READ_ROW_NOT_USED;
        goto end;
    }
    while (true) {
        TM_Result result = trx->VersionIsVisible(tuple);
        if (result == TM_Ok || result == TM_SelfUpdated) {
            if (NVMTupleDeleted(tuple)) {
                status = HAM_ROW_DELETED;
            } else {
                status = HAM_SUCCESS;
            }
            goto end;
        } else if (result == TM_Invisible || result == TM_Aborted || result == TM_BeingModified) {
            if (tuple->HasPreVersion()) {
                tuple->FetchPreVersion(trx->undoRecordCache);
            } else {
                status = HAM_NO_VISIBLE_VERSION;
                goto end;
            }
        }
    }
end:
    row_entry->Unlock();
    return status;
}

HAM_STATUS HeapUpdate(Transaction *trx, Table *table, RowId rowid, RAMTuple *tuple)
{
    Assert(table->m_rowLen == tuple->m_rowLen);
    if (CheckTrxStatus(trx)) {
        return HAM_TRANSACTION_WAIT_ABORT;
    }

    trx->PrepareUndo();
    RowIdMap *rowid_map = table->m_rowidMap;
    RowIdMapEntry *row_entry = rowid_map->GetEntry(rowid);
    char *data = row_entry->m_nvmAddr;

    NVMTuple *nvm_tuple = (NVMTuple *)data;
    row_entry->Lock();

    TM_Result result = trx->SatisifiedUpdate(nvm_tuple);
    if (result == TM_Invisible || result == TM_BeingModified) {
        row_entry->Unlock();
        trx->WaitAbort();
        return HAM_UPDATE_CONFLICT;
    } else {
        Assert(result == TM_Ok);
        if (NVMTupleDeleted(nvm_tuple)) {
            row_entry->Unlock();
            /* 一个”可见“的删除操作，说明尝试更新一个被删除的 tuple，需要报 error */
            trx->WaitAbort();
            return HAM_ROW_DELETED;
        }

        UndoColumnDesc *updated_cols = nullptr;
        uint32 update_cnt = 0;
        uint64 update_len = 0;
        tuple->GetUpdatedCols(updated_cols, update_cnt, update_len);
        UndoRecPtr undo_ptr = PrepareUpdateUndo(trx, table->SegmentHead(), rowid, nvm_tuple,
                                                UndoUpdatePara{updated_cols, update_cnt, update_len});
        tuple->InitHead(trx->GetTrxSlotLocation(), undo_ptr, nvm_tuple->m_flag1, nvm_tuple->m_flag2);
        tuple->Serialize((char *)nvm_tuple, RealTupleSize(table->GetRowLen())); /* inplace update */
        row_entry->sync_dram_cache(RealTupleSize(tuple->payload()));
        row_entry->Unlock();

        trx->PushWriteSet(row_entry);

        return HAM_SUCCESS;
    }
}

HAM_STATUS HeapDelete(Transaction *trx, Table *table, RowId rowid)
{
    if (CheckTrxStatus(trx)) {
        return HAM_TRANSACTION_WAIT_ABORT;
    }

    trx->PrepareUndo();
    RowIdMap *rowid_map = table->m_rowidMap;
    RowIdMapEntry *row_entry = rowid_map->GetEntry(rowid);
    char *data = row_entry->m_nvmAddr;

    NVMTuple *nvm_tuple = (NVMTuple *)data;
    row_entry->Lock();
    TM_Result result = trx->SatisifiedUpdate(nvm_tuple);
    if (result == TM_Invisible || result == TM_BeingModified) {
        row_entry->Unlock();
        trx->WaitAbort();
        return HAM_UPDATE_CONFLICT;
    } else {
        Assert(result == TM_Ok);
        if (NVMTupleDeleted(nvm_tuple)) {
            row_entry->Unlock();
            trx->WaitAbort();
            return HAM_ROW_DELETED;
        }

        UndoRecPtr undo_ptr = PrepareDeleteUndo(trx, table->SegmentHead(), rowid, nvm_tuple);
        NVMTupleSetDeleted(nvm_tuple);
        nvm_tuple->m_trxInfo = trx->GetTrxSlotLocation();
        nvm_tuple->m_prev = undo_ptr;
        row_entry->sync_dram_cache_deleted();
        row_entry->Unlock();
        trx->PushWriteSet(row_entry);
        return HAM_SUCCESS;
    }
}

}  // namespace NVMDB
