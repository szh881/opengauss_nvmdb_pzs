/* -------------------------------------------------------------------------
 *
 * Oplog.cpp
 *
 * Portions Copyright (c) 2023 Huawei Technologies Co.,Ltd.
 * Portions Copyright (c) 2019-2021 Virginia Tech
 *
 * IDENTIFICATION
 *	  src/gausskernel/storage/nvmdb/core/GaussDBKernel-nvmdb/third_parties/pactree/src/Oplog.cpp
 *
 * -------------------------------------------------------------------------
 */
#include <list>
#include "ordo_clock.h"
#include "pptr.h"
#include "Oplog.h"
#include "nvm_cfg.h"

constexpr int QNUM_MOD = 2;
boost::lockfree::spsc_queue<std::vector<OpStruct *> *, boost::lockfree::capacity<NVMDB::NVMDB_OPLOG_QUEUE_MAX_CAPACITY>>
    g_workQueue[NVMDB::NVMDB_MAX_GROUP * NVMDB::NVMDB_OPLOG_WORKER_THREAD_PER_GROUP];
thread_local Oplog *Oplog::perThreadLog;
int g_combinerSplits = 0;

class GlobalLogMgr {
    ThreadLogMgr *tLogMgrs{nullptr};
    std::list<int> free_list;
    std::set<int> inuse_list;
    std::mutex mtx;

public:
    GlobalLogMgr()
    {
        tLogMgrs = new ThreadLogMgr[NVMDB::NVMDB_MAX_THREAD_NUM];
        for (int i = 0; i < NVMDB::NVMDB_MAX_THREAD_NUM; i++) {
            tLogMgrs[i].core = i;
            tLogMgrs[i].perThreadLog = new Oplog();
            free_list.push_back(i);
        }
    }

    ThreadLogMgr *AllocThreadLogMgr()
    {
        std::lock_guard<std::mutex> lock_guard(mtx);
        assert(!free_list.empty());
        int idx = free_list.back();
        free_list.pop_back();
        inuse_list.insert(idx);
        assert(tLogMgrs[idx].core == idx);
        return &tLogMgrs[idx];
    }

    void RecycleThreadLogMgr(ThreadLogMgr *tLogMgr)
    {
        std::lock_guard<std::mutex> lock_guard(mtx);
        free_list.push_back(tLogMgr->core);
        inuse_list.erase(tLogMgr->core);
    }

    std::set<Oplog *> *ActiveLogMgrSet()
    {
        std::lock_guard<std::mutex> lock_guard(mtx);
        auto res = new std::set<Oplog *>();
        for (auto i : inuse_list) {
            res->insert(tLogMgrs[i].perThreadLog);
        }
        return res;
    }
};

static volatile GlobalLogMgr *g_logMgr = nullptr;
static thread_local ThreadLogMgr *g_tlogMgr = nullptr;

void InitGlobalLogMgr()
{
    if (g_logMgr == nullptr) {
        g_logMgr = new GlobalLogMgr();
    }
}

void RegisterThreadLogMgr()
{
    if (g_tlogMgr == nullptr) {
        assert(g_logMgr != nullptr);
        g_tlogMgr = const_cast<GlobalLogMgr *>(g_logMgr)->AllocThreadLogMgr();
    }
}

void ReleaseThreadLogMgr()
{
    if (g_tlogMgr != nullptr) {
        assert(g_logMgr != nullptr);
        while (!g_tlogMgr->perThreadLog->Empty()) {
            usleep(1);  // wait for combiner to remove all logs
        }
        const_cast<GlobalLogMgr *>(g_logMgr)->RecycleThreadLogMgr(g_tlogMgr);
        g_tlogMgr = nullptr;
    }
}

Oplog::Oplog()
{}

void Oplog::Enq(OpStruct *ops)
{
retry:
    unsigned long qnum = curQ % QNUM_MOD;
    while (!qLock[qnum].try_lock()) {
        std::atomic_thread_fence(std::memory_order_acq_rel);
        qnum = curQ % QNUM_MOD;
    }
    if (curQ % QNUM_MOD != qnum) {
        /* 读curQ之后，上锁之前， combine线程已经把 curQ 向前推了一个，所以应该切换下一个队列 */
        qLock[qnum].unlock();
        goto retry;
    }
    op_[qnum].push_back(ops);
    qLock[qnum].unlock();
}

void Oplog::ResetQ(int qnum)
{
    op_[qnum].clear();
}

std::vector<OpStruct *> *Oplog::getQ(int qnum)
{
    return &op_[qnum];
}

std::set<Oplog *> *CopyGlobalThreadLogs()
{
    return const_cast<GlobalLogMgr *>(g_logMgr)->ActiveLogMgrSet();
}

Oplog *Oplog::GetOpLog()
{
    RegisterThreadLogMgr();
    return g_tlogMgr->perThreadLog;
}

void Oplog::EnqPerThreadLog(OpStruct *ops)
{
    Oplog *perThrdLog = GetOpLog();
    perThrdLog->Enq(ops);
}

void Oplog::WriteOpLog(OpStruct *oplog, OpStruct::Operation op, Key_t key, void *oldNodeRawPtr, uint16_t poolId,
                       Key_t newKey, Val_t newVal)
{
    oplog->op = op;
    oplog->key = key;
    oplog->oldNodePtr = oldNodeRawPtr;  // should be persistent ptr
    oplog->poolId = poolId;
    oplog->newKey = newKey;
    oplog->newVal = newVal;
    oplog->searchLayers = (1 << PMem::GetPoolNum()) - 1;
    oplog->step = OpStruct::initial;
}

OpStruct *Oplog::allocOpLog()
{
    RegisterThreadLogMgr();
    return g_tlogMgr->allocNextLog();
}

OpStruct *ThreadLogMgr::allocNextLog()
{
    int logIdx = logCnt + NVMDB::NVMDB_NUM_LOGS_PER_THREAD * core;
    logCnt++;
    logCnt %= NVMDB::NVMDB_NUM_LOGS_PER_THREAD;

    volatile OpStruct *res = (OpStruct *)PMem::getOpLog(logIdx);
    while (res->op != OpStruct::dummy && res->op != OpStruct::done) {
        ;
    }
    return (OpStruct *)res;
}
