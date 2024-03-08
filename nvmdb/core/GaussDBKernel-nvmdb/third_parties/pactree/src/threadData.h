/* -------------------------------------------------------------------------
 *
 * threadData.h
 *
 * Portions Copyright (c) 2023 Huawei Technologies Co.,Ltd.
 * Portions Copyright (c) 2019-2021 Virginia Tech
 *
 * IDENTIFICATION
 *	  src/gausskernel/storage/nvmdb/core/GaussDBKernel-nvmdb/third_parties/pactree/src/threadData.h
 *
 * -------------------------------------------------------------------------
 */
#ifndef PACTREE_THREADS_H
#define PACTREE_THREADS_H
#include <atomic>
#include "common.h"

class ThreadData {
public:
    explicit ThreadData(int threadId)
    {
        this->threadId = threadId;
#ifdef PACTREE_ENABLE_STATS
        sltime = 0;
        dltime = 0;
#endif
    }
#ifdef PACTREE_ENABLE_STATS
    uint64_t sltime;
    uint64_t dltime;
#endif
    void SetThreadId(int thrdId)
    {
        this->threadId = thrdId;
    }
    int GetThreadId()
    {
        return this->threadId;
    }
    void Setfinish()
    {
        this->finish = true;
    }
    bool GetFinish()
    {
        return this->finish;
    }
    void SetLocalClock(uint64_t clock)
    {
        this->localClock = clock;
    }
    uint64_t GetLocalClock()
    {
        return this->localClock;
    }
    void IncrementRunCntAtomic()
    {
        runCnt.fetch_add(1);
    };
    void IncrementRunCnt()
    {
        runCnt++;
    };
    uint64_t GetRunCnt()
    {
        return this->runCnt;
    }
    void ReadLock(uint64_t clock)
    {
        this->SetLocalClock(clock);
        this->IncrementRunCntAtomic();
    }
    void ReadUnlock()
    {
        this->IncrementRunCnt();
    }
private:
    int threadId{0};
    uint64_t localClock{0};
    bool finish{false};
    volatile std::atomic<uint64_t> runCnt{0};
};

#endif
