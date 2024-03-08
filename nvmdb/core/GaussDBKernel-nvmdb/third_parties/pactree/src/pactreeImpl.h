/* -------------------------------------------------------------------------
 *
 * pactreeImpl.h
 *
 * Portions Copyright (c) 2023 Huawei Technologies Co.,Ltd.
 * Portions Copyright (c) 2019-2021 Virginia Tech
 *
 * IDENTIFICATION
 *	  src/gausskernel/storage/nvmdb/core/GaussDBKernel-nvmdb/third_parties/pactree/src/pactreeImpl.h
 *
 * -------------------------------------------------------------------------
 */
#ifndef PACTREE_IMPL_H
#define PACTREE_IMPL_H

#include <utility>
#include <vector>
#include <algorithm>
#include <thread>
#include <set>
#include "common.h"
#include "linkedList.h"
#include "SearchLayer.h"
#include "threadData.h"
#include "pactreeTrx.h"

namespace NVMDB {

extern std::vector<SearchLayer *> g_perGrpSlPtr;
extern std::set<ThreadData *> g_threadDataSet;
using DataLayer = LinkedList;

class pactreeImpl {
public:
    explicit pactreeImpl(int numGrp, root_obj *root);
    ~pactreeImpl();
    bool Insert(Key_t &key, Val_t val);
    void RegisterThread(int grpId);
    void UnregisterThread();
    Val_t Lookup(Key_t &key, bool *found);
    void Recover();

    void Scan(Key_t &startKey, Key_t &endKey, int maxRange, LookupSnapshot snapshot, bool reverse,
              std::vector<std::pair<Key_t, Val_t>> &result);
    static SearchLayer *CreateSearchLayer(root_obj *root, int threadId);
    static int GetThreadGroupId();
    static void SetThreadGroupId(int grpId);
    void Init(int numGrp, root_obj *root);

#ifdef PACTREE_ENABLE_STATS
    std::atomic<uint64_t> total_sl_time;
    std::atomic<uint64_t> total_dl_time;
#endif
private:
    DataLayer dl;
    std::vector<std::thread *> *wtArray;  // CurrentOne but there should be group number of threads
    std::thread *combinerThead;
    static thread_local int g_threadGroupId;
    void CreateWorkerThread(int numGrp, root_obj *root);
    void CreateCombinerThread();
    ListNode *getJumpNode(Key_t &key);
    static volatile int totalGroupActive;
    std::atomic<uint32_t> numThreads;
};

pactreeImpl *InitPT(const char *pmemDir);

}  // namespace NVMDB
#endif  // pactree_H
