/* -------------------------------------------------------------------------
 *
 * WorkerThread.cpp
 *
 * Portions Copyright (c) 2023 Huawei Technologies Co.,Ltd.
 * Portions Copyright (c) 2019-2021 Virginia Tech
 *
 * IDENTIFICATION
 *	  src/gausskernel/storage/nvmdb/core/GaussDBKernel-nvmdb/third_parties/pactree/src/WorkerThread.cpp
 *
 * -------------------------------------------------------------------------
 */
#include <cassert>
#include <libpmemobj.h>
#include "Oplog.h"
#include "Combiner.h"
#include "pactree.h"
#include "WorkerThread.h"

namespace NVMDB {

WorkerThread::WorkerThread(int id, int activeGrp)
{
    this->workerThreadId = id;
    this->activeGrp = activeGrp;
    this->workQueue = &g_workQueue[workerThreadId];
    this->logDoneCount = 0;
    this->opcount = 0;
    if (id == 0)
        freeQueue = new std::queue<std::pair<uint64_t, void *>>;
}

bool WorkerThread::ApplyOperation()
{
    std::vector<OpStruct *> *oplog = workQueue->front();
    int grpId = workerThreadId % activeGrp;
    SearchLayer *sl = g_perGrpSlPtr[grpId];
    uint8_t hash = static_cast<uint8_t>(workerThreadId / activeGrp);
    bool ret = false;
    for (auto opsPtr : *oplog) {
        OpStruct &ops = *opsPtr;
        if (ops.hash != hash) {
            continue;
        }
        opcount++;
        if (ops.op == OpStruct::insert) {
            void *newNodePtr =
                reinterpret_cast<void *>((static_cast<unsigned long>(ops.poolId) << 48) | ops.newNodeOid.off);
            sl->Insert(ops.key, newNodePtr);
            uint8_t remain_task = opsPtr->searchLayers.fetch_sub(sl->grpMask);
            if (remain_task == sl->grpMask) {
                opsPtr->op = OpStruct::done;
            }
        } else if (ops.op == OpStruct::remove) {
            sl->remove(ops.key, ops.oldNodePtr);
            uint8_t remain_task = opsPtr->searchLayers.fetch_sub(sl->grpMask);
            if (remain_task == sl->grpMask) {
                opsPtr->op = OpStruct::done;
            }
        } else {
            if (ops.op == OpStruct::done) {
                printf("done? %p\n", opsPtr);
            } else {
                printf("unknown op\n");
                assert(0);
                exit(1);
            }
        }
        flushToNVM((char *)opsPtr, sizeof(OpStruct));
        smp_wmb();
    }
    workQueue->pop();
    logDoneCount++;
    return ret;
}

bool WorkerThread::IsWorkQueueEmpty()
{
    return !workQueue->read_available();
}

void WorkerThread::FreeListNodes(uint64_t removeCount)
{
    assert(workerThreadId == 0 && freeQueue != NULL);
    if (freeQueue->empty()) {
        return;
    }
    while (!freeQueue->empty()) {
        std::pair<uint64_t, void *> removePair = freeQueue->front();
        if (removePair.first < removeCount) {
            PMEMoid ptr = pmemobj_oid(removePair.second);
            pmemobj_free(&ptr);
            freeQueue->pop();
        } else {
            break;
        }
    }
}

}  // namespace NVMDB
