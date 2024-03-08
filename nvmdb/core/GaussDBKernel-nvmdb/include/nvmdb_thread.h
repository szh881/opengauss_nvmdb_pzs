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
 * nvm_thread.h
 *
 * IDENTIFICATION
 *   src/gausskernel/storage/nvmdb/core/GaussDBKernel-nvmdb/include/nvm_thread.h
 * -------------------------------------------------------------------------
 */
#ifndef NVMDB_THREAD_H
#define NVMDB_THREAD_H

#include "nvm_rowid_cache.h"
#include "heap/nvm_vecrange.h"

namespace NVMDB {

struct ThreadLocalTableCache {
    RowIdCache m_rowidCache;
    VecRange m_range;
};

struct ThreadLocalStorage {
    int threadId;  // 逻辑 id
    int groupId;
};

ThreadLocalTableCache *GetThreadLocalTableCache(TableId tid);
int GetCurrentGroupId();
void InitGlobalThreadStorageMgr();
void InitThreadLocalStorage();
void DestroyThreadLocalStorage();

void InitGlobalVariables();
void DestroyGlobalVariables();
void InitThreadLocalVariables();
void DestroyThreadLocalVariables();

}  // namespace NVMDB

#endif // NVMDB_THREAD_H