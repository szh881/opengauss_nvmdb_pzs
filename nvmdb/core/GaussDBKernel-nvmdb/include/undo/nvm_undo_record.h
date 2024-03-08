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
 * nvm_undo_record.h
 *
 * IDENTIFICATION
 *   src/gausskernel/storage/nvmdb/core/GaussDBKernel-nvmdb/include/undo/nvm_undo_record.h
 * -------------------------------------------------------------------------
 */
#ifndef NVMDB_UNDO_RECORD_H
#define NVMDB_UNDO_RECORD_H

#include "nvm_types.h"
#include "nvm_undo_ptr.h"

namespace NVMDB {

static const int MAX_UNDO_RECORD_CACHE_SIZE = 4096;

typedef struct UndoRecord {
    uint16 m_undoType; // undo record 的大类
    uint16 m_rowLen; // row length
    uint16 m_deltaLen; // total delta data length
    uint32 m_seghead; // tuple 对应的 segment head
    RowId m_rowId;
    uint32 m_payload; // undo 数据长度
    UndoRecPtr m_pre;
#ifndef NDEBUG
    uint32 m_trxSlot;
    UndoRecPtr undo_ptr;
#endif
    char data[0]; // Undo 数据
} UndoRecord;

static const int UndoRecHeadSize = sizeof(UndoRecord);

UndoRecord *CopyUndoRecord(UndoRecPtr ptr, char* undo_record_cache);

enum UndoRecordType {
    InvalidUndoRecordType = 0,
    HeapInsertUndo,
    HeapUpdateUndo,
    HeapDeleteUndo,

    IndexInsertUndo,
    IndexDeleteUndo,

    MaxUndoRecordType,
};

static inline int UndoRecTotalSize(uint32 len)
{
    return len + UndoRecHeadSize;
}

static inline bool UndoRecordTypeIsValid(UndoRecordType type)
{
    return type > InvalidUndoRecordType && type < MaxUndoRecordType;
}

static_assert(MaxUndoRecordType <= MAX_UINT16);

}

#endif // NVMDB_UNDO_RECORD_H