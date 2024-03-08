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
 * nvm_rowid_cache.h
 *
 * IDENTIFICATION
 *   src/gausskernel/storage/nvmdb/core/GaussDBKernel-nvmdb/include/heap/nvm_rowid_cache.h
 * -------------------------------------------------------------------------
 */
#ifndef NVMDB_ROWID_CACHE_H
#define NVMDB_ROWID_CACHE_H

#include <vector>

#include "nvm_types.h"

namespace NVMDB {

class RowIdCache {
    std::vector<RowId> cache;

public:
    void push_back(RowId row_id)
    {
        cache.push_back(row_id);
    }

    RowId pop()
    {
        if (cache.empty()) {
            return InvalidRowId;
        }
        RowId res = cache.back();
        cache.pop_back();
        return res;
    }
};

}  // namespace NVMDB

#endif  // NVMDB_ROWID_CACHE_H