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
 * nvm_index_access.h
 *
 * IDENTIFICATION
 *   src/gausskernel/storage/nvmdb/core/GaussDBKernel-nvmdb/include/index/nvm_index_access.h
 * -------------------------------------------------------------------------
 */
#ifndef NVMDB_INDEX_ACCESS_H
#define NVMDB_INDEX_ACCESS_H

#include "index/nvm_index.h"
#include "heap/nvm_access.h"
#include "nvm_transaction.h"

namespace NVMDB {

static inline LookupSnapshot GetIndexLookupSnapshot(Transaction *trx)
{
    return {
        .snapshot = trx->GetSnapshot(),
        .min_csn = trx->GetMinSnapshot(),
    };
}
void IndexInsert(Transaction *trx, NVMIndex *index, DRAMIndexTuple *index_tuple, RowId row_id);
void IndexDelete(Transaction *trx, NVMIndex *index, DRAMIndexTuple *index_tuple, RowId row_id);

}  // namespace NVMDB

#endif