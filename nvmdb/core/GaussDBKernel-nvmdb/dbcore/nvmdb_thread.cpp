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
 * nvmdb_thread.cpp
 *
 * IDENTIFICATION
 *   src/gausskernel/storage/nvmdb/core/GaussDBKernel-nvmdb/dbcore/nvmdb_thread.cpp
 * -------------------------------------------------------------------------
 */
#include <map>

#include "nvm_rowid_map.h"
#include "nvm_transaction.h"
#include "index/nvm_index.h"
#include "nvmdb_thread.h"

namespace NVMDB {

thread_local std::map<uint32, ThreadLocalTableCache *> local_table_cache;

ThreadLocalTableCache *GetThreadLocalTableCache(uint32 seghead)
{
    if (local_table_cache.find(seghead) == local_table_cache.end()) {
        local_table_cache.insert({seghead, new ThreadLocalTableCache()});
    }

    return local_table_cache[seghead];
}

void DestroyLocalTableCache()
{
    for (auto entry : local_table_cache) {
        delete entry.second;
    }
    local_table_cache.clear();
}

struct GlobalThreadStorageMgr {
    int groupNum = 0;
    int threadNum = 0;
    std::vector<int> groupSize;
    std::mutex mtx;

    void Init(int grpId)
    {
        groupNum = grpId;
        groupSize.clear();
        for (int i = 0; i < groupNum; i++) {
            groupSize.push_back(0);
        }
    }

    void register_thrd(ThreadLocalStorage *thrd)
    {
        std::lock_guard<std::mutex> lockGuard(mtx);
        int minId = 0;
        int minNum = groupSize[0];
        for (int i = 1; i < groupNum; i++) {
            if (minNum > groupSize[i]) {
                minNum = groupSize[i];
                minId = i;
            }
        }

        thrd->groupId = minId;
        groupSize[minId]++;
        thrd->threadId = threadNum++;
    }

    void unregister_thrd(ThreadLocalStorage *thrd)
    {
        std::lock_guard<std::mutex> lockGuard(mtx);
        groupSize[thrd->groupId]--;
    }
};

struct GlobalThreadStorageMgr g_thrdMgr;

void InitGlobalThreadStorageMgr()
{
    g_thrdMgr.Init(g_dirPathNum);
}

void InitGlobalVariables()
{
    InitGlobalThreadStorageMgr();
    InitGlobalRowIdMapCache();
    InitGlobalProcArray();
}

void DestroyGlobalVariables()
{
    DestroyGlobalRowIdMapCache();
    DestroyGlobalProcArray();
}

static thread_local ThreadLocalStorage *t_storage = nullptr;

int GetCurrentGroupId()
{
    if (t_storage == nullptr) {  // for those are not registered in global manager, using group 0.
        return 0;
    }
    return t_storage->groupId;
}

void InitThreadLocalStorage()
{
    t_storage = new ThreadLocalStorage;
    g_thrdMgr.register_thrd(t_storage);
}

void DestroyThreadLocalStorage()
{
    g_thrdMgr.unregister_thrd(t_storage);
    delete t_storage;
    t_storage = nullptr;
}

void InitThreadLocalVariables()
{
    InitThreadLocalStorage();
    InitLocalRowIdMapCache();
    InitLocalUndoSegment();
    InitLocalIndex(GetCurrentGroupId());
#ifndef NVMDB_ADAPTER
    InitTransactionContext();
#endif
}

void DestroyThreadLocalVariables()
{
    DestroyThreadLocalStorage();
    DestroyLocalTableCache();
    DestroyLocalRowIdMapCache();
    DestroyLocalIndex();
#ifndef NVMDB_ADAPTER
    DestroyTransactionContext();
#endif
    DestroyLocalUndoSegment();
}

}  // namespace NVMDB
