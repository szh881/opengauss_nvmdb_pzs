/* -------------------------------------------------------------------------
 *
 * pactree.cpp
 *    pactree class defintion
 *
 * Portions Copyright (c) 2023 Huawei Technologies Co.,Ltd.
 * Portions Copyright (c) 2019-2021 Virginia Tech
 *
 * IDENTIFICATION
 *	  src/gausskernel/storage/nvmdb/core/GaussDBKernel-nvmdb/third_parties/pactree/src/pactree.cpp
 *
 * -------------------------------------------------------------------------
 */
#include <iostream>
#include <cassert>

#include <ordo_clock.h>
#include "pactreeImpl.h"
#include "Combiner.h"
#include "WorkerThread.h"
#include "nvmdb_thread.h"
#include "nvm_cfg.h"

namespace NVMDB {

constexpr int RUN_CNT_MOD = 2;
constexpr int SLEEP_TIME = 200;
constexpr int CHIP_OFFSET = 12;
constexpr int UINT_OFFSET = 32;
#ifdef PACTREE_ENABLE_STATS
#define acc_sl_time(x) (curThreadData->sltime += x)
#define acc_dl_time(x) (curThreadData->dltime += x)
#define hydralist_reset_timers() \
    do {                         \
        total_sl_time = 0;       \
        total_dl_time = 0;       \
    } while (0)
#define hydralist_start_timer()      \
    do {                             \
        unsigned long __b_e_g_i_n__; \
    __b_e_g_i_n__ = read_tsc()
#define hydralist_stop_timer(tick)        \
    (tick) += read_tsc() - __b_e_g_i_n__; \
    }                                     \
    while (0)
#else
#define ACC_SL_TIME(x)
#define ACC_DL_TIME(x)
#define HYDRALIST_START_TIMER()
#define HYDRALIST_STOP_TIMER(tick)
#define HYDRALIST_RESET_TIMERS()
#endif

std::vector<WorkerThread *> g_WorkerThreadInst(NVMDB_MAX_GROUP *NVMDB_OPLOG_WORKER_THREAD_PER_GROUP);
std::vector<SearchLayer *> g_perGrpSlPtr(NVMDB_MAX_GROUP);
std::set<ThreadData *> g_threadDataSet;
std::atomic<bool> g_globalStop;
std::atomic<bool> g_combinerStop;
thread_local int pactreeImpl::g_threadGroupId = -1;
thread_local ThreadData *g_curThreadData = nullptr;
volatile int pactreeImpl::totalGroupActive = 0;
volatile bool threadInitialized[NVMDB_MAX_GROUP];
volatile bool slReady[NVMDB_MAX_GROUP];
std::mutex g_threadDataLock;
uint64_t g_removeCount;
volatile std::atomic<bool> g_removeDetected;

void workerThreadExec(int threadId, int activeGrp, root_obj *root)
{
    ALWAYS_CHECK(activeGrp > 0);
    auto thread_name = std::string("worker thread_") + std::to_string(threadId);
    pthread_setname_np(pthread_self(), thread_name.c_str());
    int grpId = threadId % activeGrp;
    pactreeImpl::SetThreadGroupId(threadId);
    WorkerThread wt(threadId, activeGrp);
    g_WorkerThreadInst[threadId] = &wt;
    if (threadId < activeGrp) {
        while (threadInitialized[threadId] == 0) {
            ;
        }
        slReady[threadId] = false;
        g_perGrpSlPtr[threadId] = pactreeImpl::CreateSearchLayer(root, threadId);
        g_perGrpSlPtr[threadId]->SetGroupId(threadId);
        slReady[threadId] = true;
    }
    int count = 0;
    uint64_t lastRemoveCount = 0;
    while (!g_combinerStop) {
        usleep(1);
        while (!wt.IsWorkQueueEmpty()) {
            count++;
            wt.ApplyOperation();
        }
        if (threadId == 0 && lastRemoveCount != g_removeCount) {
        }
    }
    while (!wt.IsWorkQueueEmpty()) {
        count++;
        if (wt.ApplyOperation() && !g_removeDetected) {
            g_removeDetected.store(true);
        }
    }
    // If worker threads are stopping that means there are no more
    // user threads
    if (threadId == 0) {
        wt.FreeListNodes(ULLONG_MAX);
    }
    g_WorkerThreadInst[threadId] = NULL;
}

uint64_t gracePeriodInit(std::vector<ThreadData *> &threadsToWait)
{
    uint64_t curTime = ordo_get_clock();
    for (auto td : g_threadDataSet) {
        if (td->GetFinish()) {
            g_threadDataLock.lock();
            g_threadDataSet.erase(td);
            g_threadDataLock.unlock();
            free(td);
            continue;
        }
        if (td->GetRunCnt() % RUN_CNT_MOD) {
            if (ordo_gt_clock(td->GetLocalClock(), curTime))
                continue;
            else
                threadsToWait.push_back(td);
        }
    }
    return curTime;
}
void waitForThreads(std::vector<ThreadData *> &threadsToWait, uint64_t gpStartTime)
{
    for (int i = 0; i < threadsToWait.size(); i++) {
        if (threadsToWait[i] == NULL)
            continue;
        ThreadData *td = threadsToWait[i];
        if (td->GetRunCnt() % RUN_CNT_MOD == 0)
            continue;
        if (ordo_gt_clock(td->GetLocalClock(), gpStartTime))
            continue;
        while (td->GetRunCnt() % RUN_CNT_MOD) {
            usleep(1);
            std::atomic_thread_fence(std::memory_order::memory_order_acq_rel);
        }
    }
}
void BroadcastDoneCount(uint64_t removeCount)
{
    g_removeCount = removeCount;
}
void CombinerThreadExec(int activeGrp)
{
    pthread_setname_np(pthread_self(), "combinerThread");
    CombinerThread ct;
    int count = 0;
    while (!g_globalStop) {
        std::vector<OpStruct *> *mergedLog = ct.combineLogs();
        if (mergedLog != nullptr) {
            count++;
            ct.broadcastMergedLog(mergedLog, activeGrp);
        }
        uint64_t doneCountWt = ct.FreeMergedLogs(activeGrp, false);
        std::vector<ThreadData *> threadsToWait;
        if (g_removeDetected && doneCountWt != 0) {
            uint64_t gpStartTime = gracePeriodInit(threadsToWait);
            waitForThreads(threadsToWait, gpStartTime);
            BroadcastDoneCount(doneCountWt);
            g_removeDetected.store(false);
        } else {
            usleep(1);
        }
    }
    int i = 20;
    while (i--) {
        usleep(SLEEP_TIME);
        std::vector<OpStruct *> *mergedLog = ct.combineLogs();
        if (mergedLog != nullptr) {
            count++;
            ct.broadcastMergedLog(mergedLog, activeGrp);
        }
    }
    while (!ct.MergedLogsToBeFreed()) {
        ct.FreeMergedLogs(activeGrp, true);
    }
    g_combinerStop = true;
}

void pactreeImpl::CreateWorkerThread(int numGrp, root_obj *root)
{
    for (int i = 0; i < numGrp * NVMDB_OPLOG_WORKER_THREAD_PER_GROUP; i++) {
        threadInitialized[i % numGrp] = false;
        std::thread *wt = new std::thread(workerThreadExec, i, numGrp, root);
        usleep(1);
        wtArray->push_back(wt);
        threadInitialized[i % numGrp] = true;
    }
}

void pactreeImpl::CreateCombinerThread()
{
    combinerThead = new std::thread(CombinerThreadExec, totalGroupActive);
}

pactreeImpl *InitPT(const char *pmemDir)
{
    root_obj *root = nullptr;
    root_obj *sl_root = nullptr;
    int isCreate;
    PMem::CreatePMemPool(pmemDir, &isCreate, &root, &sl_root);
    int poolNum = PMem::GetPoolNum();
    if (isCreate == 0) {
        pactreeImpl *pt = (pactreeImpl *)pmemobj_direct(root->ptr[0]);
        pt->Init(poolNum, sl_root);
        return pt;
    }
    PMEMobjpool *pop = (PMEMobjpool *)PMem::getBaseOf(1);

    pmemobj_alloc(pop, &(root->ptr[0]), sizeof(pactreeImpl), 0, NULL, NULL);
    void *rootVaddr = pmemobj_direct(root->ptr[0]);
    pactreeImpl *pt = (pactreeImpl *)new (rootVaddr) pactreeImpl(poolNum, sl_root);
    flushToNVM((char *)root, sizeof(root_obj));
    smp_wmb();
    return pt;
}

void pactreeImpl::Init(int numGrp, root_obj *root)
{
    int id = 0;
    InitGlobalLogMgr();
    totalGroupActive = numGrp;
    wtArray = new std::vector<std::thread *>;
    g_WorkerThreadInst.clear();
    g_perGrpSlPtr.clear();
    g_threadDataSet.clear();

    g_perGrpSlPtr.resize(totalGroupActive);
    g_globalStop = false;
    g_combinerStop = false;
    CreateWorkerThread(numGrp, root);
    CreateCombinerThread();
    for (int i = 0; i < totalGroupActive; i++) {
        while (slReady[i] == false) {
            ;
        }
    }
    RegisterThread(0);
    Recover();
    UnregisterThread();
    HYDRALIST_RESET_TIMERS();
}

pactreeImpl::pactreeImpl(int numGrp, root_obj *root)
{
    totalGroupActive = numGrp;
    InitGlobalLogMgr();
    wtArray = new std::vector<std::thread *>;
    g_perGrpSlPtr.resize(totalGroupActive);
    dl.Initialize();
    // need to read from PM

    g_globalStop = false;
    g_combinerStop = false;

    CreateWorkerThread(numGrp, root);
    CreateCombinerThread();
    for (int i = 0; i < totalGroupActive; i++) {
        while (slReady[i] == false) {
            ;
        }
    }
    HYDRALIST_RESET_TIMERS();
}

pactreeImpl::~pactreeImpl()
{
    g_globalStop = true;
    for (auto &t : *wtArray)
        t->join();
    combinerThead->join();
}

ListNode *pactreeImpl::getJumpNode(Key_t &key)
{
    int grpId = GetThreadGroupId();
    ALWAYS_CHECK(grpId == NVMDB::GetCurrentGroupId());
    SearchLayer &sl = *g_perGrpSlPtr[grpId];
    if (sl.IsEmpty()) {
        return dl.GetHead();
    }
    ListNode *jumpNode = reinterpret_cast<ListNode *>(sl.lookup(key));
    if (jumpNode == nullptr)
        jumpNode = dl.GetHead();
    return jumpNode;
}

#ifdef SYNC

// lock interface
ListNode *pactreeImpl::getJumpNodewithLock(Key_t &key, void **node)
{
    SearchLayer &sl = *g_perGrpSlPtr[0];
    if (sl.isEmpty())
        return dl.getHead();
    auto *jumpNode = reinterpret_cast<ListNode *>(sl.lookupwithLock(key, node));
    if (jumpNode == nullptr) {
        jumpNode = dl.getHead();
    }
    return jumpNode;
}

// unlock interface
bool pactreeImpl::JumpNodewithUnLock(void *node)
{
    SearchLayer &sl = *g_perGrpSlPtr[0];

    return sl.NodeUnlock(node);
}
#endif

bool pactreeImpl::Insert(Key_t &key, Val_t val)
{
    uint64_t clock = ordo_get_clock();
    g_curThreadData->ReadLock(clock);
    uint64_t ticks = 0;

    int threadId = g_curThreadData->GetThreadId();
    bool ret;
    HYDRALIST_START_TIMER();
    ListNode *jumpNode = getJumpNode(key);

    HYDRALIST_STOP_TIMER(ticks);
    HYDRALIST_START_TIMER();
    ACC_SL_TIME(ticks);
    ret = dl.Insert(key, val, jumpNode, threadId);

    HYDRALIST_STOP_TIMER(ticks);
    ACC_DL_TIME(ticks);
    g_curThreadData->ReadUnlock();

    return ret;
}

Val_t pactreeImpl::Lookup(Key_t &key, bool *found)
{
    uint64_t clock = ordo_get_clock();
    g_curThreadData->ReadLock(clock);
    Val_t val;
    uint64_t ticks;

    ListNode *jumpNode = getJumpNode(key);

    *found = dl.Lookup(key, val, jumpNode);
    g_curThreadData->ReadUnlock();
    return val;
}

SearchLayer *pactreeImpl::CreateSearchLayer(root_obj *root, int threadId)
{
    if (pmemobj_direct(root->ptr[threadId]) == nullptr) {
        pptr<SearchLayer> sPtr;
        PMem::alloc(sizeof(SearchLayer), (void **)&sPtr, &(root->ptr[threadId]));
        SearchLayer *s = new (sPtr.getVaddr()) SearchLayer();
        return s;
    } else {
        SearchLayer *s = (SearchLayer *)pmemobj_direct(root->ptr[threadId]);
        s->Init();
        return s;
    }

    // Read from Root Object or Allocate new one.
}

unsigned long TaccRdtscp(int *chip, int *core)
{
    unsigned long a;
    unsigned long d;
    unsigned long c;
    __asm__ volatile("rdtscp" : "=a"(a), "=d"(d), "=c"(c));
    *chip = (c & 0xFFF000) >> CHIP_OFFSET;
    *core = c & 0xFFF;
    return (static_cast<unsigned long>(a)) | ((static_cast<unsigned long>(d)) << UINT_OFFSET);
    ;
}

int pactreeImpl::GetThreadGroupId()
{
    if (g_threadGroupId == -1) {
        return 0;
    }
    ALWAYS_CHECK(g_threadGroupId < totalGroupActive);
    return g_threadGroupId;
}

void pactreeImpl::SetThreadGroupId(int grpId)
{
    g_threadGroupId = grpId;
}

void pactreeImpl::Scan(Key_t &startKey, Key_t &endKey, int maxRange, LookupSnapshot snapshot, bool reverse,
                       std::vector<std::pair<Key_t, Val_t>> &result)
{
    do {
        if (reverse) {
            assert(0);
        } else {
            ListNode *jumpNode = getJumpNode(startKey);
            if (dl.ScanInOrder(startKey, endKey, jumpNode, maxRange, snapshot, result)) {
                return;
            }
        }
    } while (true);
}

void pactreeImpl::RegisterThread(int grpId)
{
    SetThreadGroupId(grpId);
    int threadId = numThreads.fetch_add(1);
    auto td = new ThreadData(threadId);
    g_threadDataLock.lock();
    g_threadDataSet.insert(td);
    g_threadDataLock.unlock();
    g_curThreadData = td;
    std::atomic_thread_fence(std::memory_order_acq_rel);
}

void pactreeImpl::UnregisterThread()
{
    if (g_curThreadData == nullptr) {
        return;
    }
    ReleaseThreadLogMgr();
    int threadId = g_curThreadData->GetThreadId();
    g_curThreadData->Setfinish();
}

void pactreeImpl::Recover()
{
    for (int i = 0; i < totalGroupActive; i++) {
        dl.Recover(g_perGrpSlPtr[i]);
    }
}

}  // namespace NVMDB