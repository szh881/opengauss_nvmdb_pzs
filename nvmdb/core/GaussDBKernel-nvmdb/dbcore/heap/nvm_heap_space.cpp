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
 * nvm_heap_space.cpp
 *
 * IDENTIFICATION
 *   src/gausskernel/storage/nvmdb/core/GaussDBKernel-nvmdb/dbcore/heap/nvm_heap_space.cpp
 * -------------------------------------------------------------------------
 */
#include "nvm_heap_space.h"

namespace NVMDB {

static const char HEAP_FILENAME[] = "heap";
TableSpace *g_heapSpace = nullptr;

void HeapCreate(const char *dir)
{
    g_heapSpace = new TableSpace(dir, HEAP_FILENAME);
    g_heapSpace->Create();
}
void HeapBootStrap(const char *dir)
{
    g_heapSpace = new TableSpace(dir, HEAP_FILENAME);
    g_heapSpace->Mount();
}

void HeapExitProcess()
{
    if (g_heapSpace != nullptr) {
        g_heapSpace->UnMount();
        delete g_heapSpace;
        g_heapSpace = nullptr;
    }
}

}  // namespace NVMDB