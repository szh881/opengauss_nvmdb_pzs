/* -------------------------------------------------------------------------
 *
 * pactreeTrx.h
 *
 * Portions Copyright (c) 2023 Huawei Technologies Co.,Ltd.
 * Portions Copyright (c) 2019-2021 Virginia Tech
 *
 * IDENTIFICATION
 *	  src/gausskernel/storage/nvmdb/core/GaussDBKernel-nvmdb/third_parties/pactree/src/pactreeTrx.h
 *
 * -------------------------------------------------------------------------
 */
#ifndef NVMDB_PACTREE_TRANSACTION_H
#define NVMDB_PACTREE_TRANSACTION_H

#include "heap/nvm_tuple.h"
#include "undo/nvm_undo_segment.h"

namespace NVMDB {

struct LookupSnapshot {
    uint64 snapshot;  // 当前事务的csn
    uint64 min_csn;   // 回收
};

}  // namespace NVMDB

#endif  // NVMDB_TRANSACTION_H
