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
 * nvm_access.h
 *
 * IDENTIFICATION
 *   src/gausskernel/storage/nvmdb/core/GaussDBKernel-nvmdb/include/heap/nvm_access.h
 * -------------------------------------------------------------------------
 */
#ifndef NVMDB_HEAP_ACCESS_H
#define NVMDB_HEAP_ACCESS_H

#include "nvm_types.h"
#include "nvm_transaction.h"
#include "nvm_table.h"

namespace NVMDB {

/* heap access method status */
enum HAM_STATUS {
    HAM_SUCCESS,
    HAM_READ_ROW_NOT_USED,       // the rowid is not used
    HAM_NO_VISIBLE_VERSION,      // there is no visible version
    HAM_UPDATE_CONFLICT,         // another transaction are updating this version
    HAM_ROW_DELETED,             // the row is deleted
    HAM_TRANSACTION_WAIT_ABORT,  // an error happens so the transaction has to be aborted
};

RowId HeapUpperRowId(Table *table);

RowId HeapInsert(Transaction *trx, Table *table, RAMTuple *tuple);

HAM_STATUS HeapRead(Transaction *trx, Table *table, RowId rowid, RAMTuple *tuple);

HAM_STATUS HeapUpdate(Transaction *trx, Table *table, RowId rowid, RAMTuple *new_tuple);

HAM_STATUS HeapDelete(Transaction *trx, Table *table, RowId rowid);

}  // namespace NVMDB

#endif  // NVMDB_HEAP_ACCESS_H