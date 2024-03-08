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
 * nvm_heap_space.h
 *
 * IDENTIFICATION
 *   src/gausskernel/storage/nvmdb/core/GaussDBKernel-nvmdb/include/heap/nvm_heap_space.h
 * -------------------------------------------------------------------------
 */
#ifndef NVMDB_HEAP_SPACE_H
#define NVMDB_HEAP_SPACE_H

#include "nvm_table_space.h"

namespace NVMDB {

void HeapCreate(const char *dir);

void HeapBootStrap(const char *dir);

void HeapExitProcess();

extern TableSpace *g_heapSpace;

}  // namespace NVMDB

#endif  // NVMDB_HEAP_SPACE_H