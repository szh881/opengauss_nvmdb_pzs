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
 * nvm_undo_ptr.h
 *
 * IDENTIFICATION
 *   src/gausskernel/storage/nvmdb/core/GaussDBKernel-nvmdb/include/undo/nvm_undo_ptr.h
 * -------------------------------------------------------------------------
 */
#ifndef NVMDB_UNDO_PTR_H
#define NVMDB_UNDO_PTR_H

#include "nvm_types.h"

namespace NVMDB {

typedef uint64 UndoRecPtr;
static int UNDO_REC_PTR_OFFSET_BITS = 48;
static const UndoRecPtr InvalidUndoRecPtr = 0;

static UndoRecPtr AssembleUndoRecPtr(uint64 segid, uint64 offset)
{
    Assert((offset >> UNDO_REC_PTR_OFFSET_BITS) == 0);
    return (segid << UNDO_REC_PTR_OFFSET_BITS) | offset;
}

static inline bool UndoRecPtrIsInValid(UndoRecPtr undo)
{
    return undo == 0;
}

static inline uint32 UndoRecPtrGetSegment(UndoRecPtr undo)
{
    return undo >> UNDO_REC_PTR_OFFSET_BITS;
}

static inline uint64 UndoRecPtrGetOffset(UndoRecPtr undo)
{
    return undo & ((1LLU << UNDO_REC_PTR_OFFSET_BITS) - 1);
}

}

#endif