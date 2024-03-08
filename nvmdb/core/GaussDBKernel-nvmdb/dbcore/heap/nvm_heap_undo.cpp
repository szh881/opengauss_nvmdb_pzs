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
 * nvm_heap_undo.cpp
 *
 * IDENTIFICATION
 *   src/gausskernel/storage/nvmdb/core/GaussDBKernel-nvmdb/dbcore/heap/nvm_heap_undo.cpp
 * -------------------------------------------------------------------------
 */
#include <cstring>

#include "nvm_undo_api.h"
#include "nvm_transaction.h"
#include "nvm_vecstore.h"

namespace NVMDB {
static constexpr size_t UNDO_DATA_MAX_SIZE = MAX_UNDO_RECORD_CACHE_SIZE - NVMTupleHeadSize;

UndoRecPtr PrepareInsertUndo(Transaction *trx, uint32 seghead, RowId rowId, uint16 rowLen)
{
    auto *undo = reinterpret_cast<UndoRecord *>(trx->undoRecordCache);
    undo->m_undoType = HeapInsertUndo;
    undo->m_rowLen = rowLen;
    undo->m_seghead = seghead;
    undo->m_rowId = rowId;
    undo->m_payload = 0;
    undo->m_pre = 0;
#ifndef NDEBUG
    undo->m_trxSlot = trx->GetTrxSlotLocation();
#endif
    UndoRecPtr undoPtr = trx->InsertUndoRecord(undo);
    return undoPtr;
}

#define DELTA_UNDO_HEAD (sizeof(UndoColumnDesc))

/* offset | length | data */
static inline uint64 DeltaUndoSize(uint32 updateCnt, uint64 updateLen)
{
    return DELTA_UNDO_HEAD * updateCnt + updateLen;
}

inline void PackDeltaUndo(char *rowData, UndoColumnDesc *updatedCols, uint32 updateCnt, uint64 updateLen,
                          char *packData)
{
    char *packDataEnd = packData + UNDO_DATA_MAX_SIZE;
    Assert(packData != nullptr);
    for (uint32 i = 0; i < updateCnt; i++) {
        int ret = memcpy_s(packData, packDataEnd - packData, &updatedCols[i], DELTA_UNDO_HEAD);
        SecureRetCheck(ret);
        packData += DELTA_UNDO_HEAD;
        ret = memcpy_s(packData, packDataEnd - packData, rowData + updatedCols[i].m_colOffset, updatedCols[i].m_colLen);
        SecureRetCheck(ret);
        packData += updatedCols[i].m_colLen;
    }
}

inline void UnpackDeltaUndo(char *rowData, char *packData, uint64 deltaLen)
{
    UndoColumnDesc updatedCol;
    while (deltaLen > 0) {
        int ret = memcpy_s(&updatedCol, DELTA_UNDO_HEAD, packData, DELTA_UNDO_HEAD);
        SecureRetCheck(ret);
        packData += DELTA_UNDO_HEAD;
        ret = memcpy_s(rowData + updatedCol.m_colOffset, updatedCol.m_colLen, packData, updatedCol.m_colLen);
        SecureRetCheck(ret);
        packData += updatedCol.m_colLen;
        deltaLen -= (DELTA_UNDO_HEAD + updatedCol.m_colLen);
    }
    Assert(deltaLen == 0);
}

UndoRecPtr PrepareUpdateUndo(Transaction *trx, uint32 seghead, RowId rowid, NVMTuple *oldTuple,
                             const UndoUpdatePara &para)
{
    uint64 deltaLen = DeltaUndoSize(para.m_updateCnt, para.m_updateLen);
    auto *undo = reinterpret_cast<UndoRecord *>(trx->undoRecordCache);
    undo->m_undoType = HeapUpdateUndo;
    undo->m_rowLen = oldTuple->m_len;
    undo->m_deltaLen = deltaLen;
    undo->m_seghead = seghead;
    undo->m_rowId = rowid;
    undo->m_payload = NVMTupleHeadSize + deltaLen;
    undo->m_pre = 0;
#ifndef NDEBUG
    undo->m_trxSlot = trx->GetTrxSlotLocation();
#endif
    int ret = memcpy_s(undo->data, MAX_UNDO_RECORD_CACHE_SIZE, oldTuple, NVMTupleHeadSize);
    SecureRetCheck(ret);
    PackDeltaUndo(oldTuple->data, para.m_updatedCols, para.m_updateCnt, para.m_updateLen,
                  undo->data + NVMTupleHeadSize);
    UndoRecPtr undoPtr = trx->InsertUndoRecord(undo);

    return undoPtr;
}

UndoRecPtr PrepareDeleteUndo(Transaction *trx, uint32 seghead, RowId rowid, NVMTuple *oldTuple)
{
    auto *undo = reinterpret_cast<UndoRecord *>(trx->undoRecordCache);
    undo->m_undoType = HeapDeleteUndo;
    undo->m_rowLen = oldTuple->m_len;
    undo->m_seghead = seghead;
    undo->m_rowId = rowid;
    undo->m_payload = oldTuple->m_len + NVMTupleHeadSize;
    undo->m_pre = 0;
#ifndef NDEBUG
    undo->m_trxSlot = trx->GetTrxSlotLocation();
#endif
    int ret = memcpy_s(undo->data, MAX_UNDO_RECORD_CACHE_SIZE, oldTuple, undo->m_payload);
    SecureRetCheck(ret);
    UndoRecPtr undoPtr = trx->InsertUndoRecord(undo);
    return undoPtr;
}

void UndoInsert(UndoRecord *undo)
{
    RowIdMap *rowidMap = GetRowIdMap(undo->m_seghead, undo->m_rowLen);
    RowIdMapEntry *row = rowidMap->GetEntry(undo->m_rowId);
    row->Lock();
    NVMTupleSetUnUsed(reinterpret_cast<NVMTuple *>(row->m_nvmAddr));
    if (row->m_dramCache != nullptr) {
        NVMTupleSetUsed(reinterpret_cast<NVMTuple *>(row->m_dramCache));
    }
    row->Unlock();
}

void UndoUpdate(UndoRecord *undo)
{
    RowIdMap *rowidMap = GetRowIdMap(undo->m_seghead, undo->m_rowLen);
    RowIdMapEntry *row = rowidMap->GetEntry(undo->m_rowId);
    row->Lock();
    int ret = memcpy_s(row->m_nvmAddr, RealTupleSize(undo->m_rowLen), undo->data, NVMTupleHeadSize);
    SecureRetCheck(ret);
    UnpackDeltaUndo(row->m_nvmAddr + NVMTupleHeadSize, undo->data + NVMTupleHeadSize, undo->m_deltaLen);
    if (row->m_dramCache != nullptr) {
        ret = memcpy_s(row->m_dramCache, RealTupleSize(undo->m_rowLen), row->m_nvmAddr,
                       undo->m_rowLen + NVMTupleHeadSize);
        SecureRetCheck(ret);
    }
    row->Unlock();
}

void UndoUpdate(UndoRecord *undo, RAMTuple *tuple)
{
    int ret = memcpy_s(static_cast<NVMTuple *>(tuple), sizeof(NVMTuple), undo->data, NVMTupleHeadSize);
    SecureRetCheck(ret);
    UnpackDeltaUndo(tuple->m_rowData, undo->data + NVMTupleHeadSize, undo->m_deltaLen);
    tuple->m_isNullBitmap = tuple->m_null;
}

void UndoDelete(UndoRecord *undo)
{
    RowIdMap *rowidMap = GetRowIdMap(undo->m_seghead, undo->m_rowLen);
    RowIdMapEntry *row = rowidMap->GetEntry(undo->m_rowId);
    row->Lock();
    int ret = memcpy_s(row->m_nvmAddr, undo->m_rowLen + NVMTupleHeadSize, undo->data, undo->m_payload);
    SecureRetCheck(ret);
    if (row->m_dramCache != nullptr) {
        ret = memcpy_s(row->m_dramCache, RealTupleSize(undo->m_rowLen), undo->data, undo->m_payload);
        SecureRetCheck(ret);
    }
    row->Unlock();
}

}  // namespace NVMDB