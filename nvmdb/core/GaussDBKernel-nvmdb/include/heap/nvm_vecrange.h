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
 * nvm_vecrange.h
 *
 * IDENTIFICATION
 *   src/gausskernel/storage/nvmdb/core/GaussDBKernel-nvmdb/include/heap/nvm_vecrange.h
 * -------------------------------------------------------------------------
 */
#ifndef NVMDB_VECRANGE_H
#define NVMDB_VECRANGE_H

#include "nvm_types.h"

namespace NVMDB {

struct VecRange {
    RowId start;
    RowId end;

    VecRange()
    {
        start = end = InvalidRowId;
    }

    bool empty() const
    {
        return start >= end;
    }

    RowId next()
    {
        if (start < end) {
            return start++;
        }
        return InvalidRowId;
    }
};

}  // namespace NVMDB

#endif // NVMDB_VECRANGE_H