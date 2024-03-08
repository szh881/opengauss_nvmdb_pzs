/* -------------------------------------------------------------------------
 *
 * WorkerThread.h
 *
 * Portions Copyright (c) 2023 Huawei Technologies Co.,Ltd.
 * Portions Copyright (c) 2019-2021 Virginia Tech
 *
 * IDENTIFICATION
 *	  src/gausskernel/storage/nvmdb/core/GaussDBKernel-nvmdb/third_parties/pactree/src/WorkerThread.h
 *
 * -------------------------------------------------------------------------
 */
#ifndef PACTREE_WORKERTHREAD_H
#define PACTREE_WORKERTHREAD_H
#include <queue>
#include "common.h"
#include "Oplog.h"

namespace NVMDB {

extern int g_lockCapacity;

class WorkerThread {
public:
    unsigned long opcount{0};
    WorkerThread(int id, int activeGrp);
    bool ApplyOperation();
    bool IsWorkQueueEmpty();
    unsigned long GetLogDoneCount()
    {
        return logDoneCount;
    }
    void FreeListNodes(uint64_t removeCount);
private:
    boost::lockfree::spsc_queue<std::vector<OpStruct *> *, boost::lockfree::capacity<LOCK_CAPACITY>> *workQueue;
    int workerThreadId{0};
    int activeGrp{0};
    unsigned long logDoneCount{0};
    std::queue<std::pair<uint64_t, void *>> *freeQueue;
};
extern std::vector<WorkerThread *> g_WorkerThreadInst;

}  // namespace NVMDB

#endif
