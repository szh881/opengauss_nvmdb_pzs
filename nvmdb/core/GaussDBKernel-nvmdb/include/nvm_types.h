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
 * nvm_types.h
 *
 * IDENTIFICATION
 *   src/gausskernel/storage/nvmdb/core/GaussDBKernel-nvmdb/include/nvm_types.h
 * -------------------------------------------------------------------------
 */
#ifndef NVMDB_TYPES_H
#define NVMDB_TYPES_H

#include "nvm_utils.h"

namespace NVMDB {

using uint64 = unsigned long long;
using uint32 = unsigned int;
using uint16 = unsigned short;
using uint8 = unsigned char;
using int32 = signed int;
using int64 = signed long long;

static const uint32 MAX_UINT32 = 0xffffffff;
static const uint16 MAX_UINT16 = 0xffff;
static const uint8 MAX_UINT8 = 0xff;

static const size_t MAX_TUPLE_LEN = 8192;

typedef uint64 PointerOffset;
typedef uint32 BlockNumber;

typedef uint32 RowId;
static const RowId InvalidRowId = 0xFFFFFFFF;
static const RowId MaxRowId = InvalidRowId - 1;
#define RowIdIsValid(x) ((x) != InvalidRowId)

typedef uint32 TableId;
static const TableId InvalidTableId = 0xFFFFFFFF;

static const uint32 InvalidColId = 0xFFFFFFFF;

static const uint32 BIS_PER_BYTE = 8;
static const uint32 BIS_PER_U32 = BIS_PER_BYTE * sizeof(uint32);
}  // namespace NVMDB

#endif // NVMDB_TYPES_H