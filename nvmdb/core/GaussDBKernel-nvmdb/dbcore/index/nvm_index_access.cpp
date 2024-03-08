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
 * nvm_index_access.cpp
 *
 * IDENTIFICATION
 *   src/gausskernel/storage/nvmdb/core/GaussDBKernel-nvmdb/dbcore/index/nvm_index_access.cpp
 * -------------------------------------------------------------------------
 */
#include "index/nvm_index_access.h"
#include "index/nvm_index_undo.h"

namespace NVMDB {

/* Index 只能返回某一个 key 和对应的 RowID，但是并不能做可见性判断，所以需要有一层 IndexAccess
 * ，在Index基础上，做事务的可见性判断过滤 */
void IndexInsert(Transaction *trx, NVMIndex *index, DRAMIndexTuple *indexTuple, RowId rowId)
{
    Key_t key;
    trx->PrepareUndo();
    index->Encode(indexTuple, &key, rowId);
    PrepareIndexInsertUndo(trx, key, trx->GetSnapshot());
    index->Insert(indexTuple, rowId);
}

void IndexDelete(Transaction *trx, NVMIndex *index, DRAMIndexTuple *indexTuple, RowId rowId)
{
    Key_t key;
    trx->PrepareUndo();
    index->Encode(indexTuple, &key, rowId);
    PrepareIndexDeleteUndo(trx, key);
    index->Delete(indexTuple, rowId, trx->GetTrxSlotLocation());
}
}  // namespace NVMDB
