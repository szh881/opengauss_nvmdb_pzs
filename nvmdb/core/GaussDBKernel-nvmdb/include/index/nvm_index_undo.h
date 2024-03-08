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
 * nvm_index_undo.h
 *
 * IDENTIFICATION
 *   src/gausskernel/storage/nvmdb/core/GaussDBKernel-nvmdb/include/index/nvm_index_undo.h
 * -------------------------------------------------------------------------
 */
#ifndef NVMDB_INDEX_UNDO_H
#define NVMDB_INDEX_UNDO_H

#include "index/nvm_index.h"
#include "nvm_transaction.h"

namespace NVMDB {

void PrepareIndexInsertUndo(Transaction *trx, Key_t &key, uint64 CSN);

void PrepareIndexDeleteUndo(Transaction *trx, Key_t &key);

void UndoIndexInsert(UndoRecord *undo);

void UndoIndexDelete(UndoRecord *undo);

}  // namespace NVMDB

#endif  // NVMDB_INDEX_UNDO_H
