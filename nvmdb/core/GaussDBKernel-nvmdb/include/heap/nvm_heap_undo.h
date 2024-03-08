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
 * nvm_heap_undo.h
 *
 * IDENTIFICATION
 *   src/gausskernel/storage/nvmdb/core/GaussDBKernel-nvmdb/include/heap/nvm_heap_undo.h
 * -------------------------------------------------------------------------
 */
#ifndef NVMDB_HEAP_UNDO_H
#define NVMDB_HEAP_UNDO_H

#include "nvm_types.h"
#include "nvm_undo_ptr.h"
#include "nvm_transaction.h"

namespace NVMDB {

UndoRecPtr PrepareInsertUndo(Transaction *trx, uint32 seghead, RowId rowid, uint16 row_len);

UndoRecPtr PrepareUpdateUndo(Transaction *trx, uint32 seghead, RowId rowid, NVMTuple *old_tuple,
                             const UndoUpdatePara &para);

UndoRecPtr PrepareDeleteUndo(Transaction *trx, uint32 seghead, RowId rowid, NVMTuple *old_tuple);

void UndoInsert(UndoRecord *undo);

void UndoUpdate(UndoRecord *undo);

void UndoDelete(UndoRecord *undo);

void UndoUpdate(UndoRecord *undo, RAMTuple *tuple);

}  // namespace NVMDB

#endif  // NVMDB_HEAP_UNDO_H