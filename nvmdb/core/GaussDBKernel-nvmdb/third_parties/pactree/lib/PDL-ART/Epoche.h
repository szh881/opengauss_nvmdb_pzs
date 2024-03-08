/* -------------------------------------------------------------------------
 *
 * Epoche.h
 *
 * Portions Copyright (c) 2023 Huawei Technologies Co.,Ltd.
 * Portions Copyright (c) 2019-2021 Virginia Tech
 * Portions Copyright (c) 2014-2015 Florian Scheibner
 *
 * IDENTIFICATION
 *	  src/gausskernel/storage/nvmdb/core/GaussDBKernel-nvmdb/third_parties/pactree/lib/PDL-ART/Epoche.h
 *
 * -------------------------------------------------------------------------
 */
#ifndef ART_EPOCHE_H
#define ART_EPOCHE_H

#include <atomic>
#include <array>
#include "tbb/enumerable_thread_specific.h"
#include "tbb/combinable.h"

namespace ART {

struct LabelDelete {
    static constexpr size_t defaultArraySize = 32;
    std::array<void *, defaultArraySize> nodes;
    uint64_t epoche;
    std::size_t nodesCount;
    LabelDelete *next;
};

class DeletionList {
    LabelDelete *headDeletionList = nullptr;
    LabelDelete *freeLabelDeletes = nullptr;
    std::size_t deletitionListCount = 0;

public:
    std::atomic<uint64_t> localEpoche;
    size_t thresholdCounter{0};

    ~DeletionList();

    LabelDelete *Head();

    void Add(void *n, uint64_t globalEpoch);

    void Remove(LabelDelete *label, LabelDelete *prev);

    std::size_t Size();

    std::uint64_t deleted = 0;
    std::uint64_t added = 0;
};

class Epoche;
class EpocheGuard;

class ThreadInfo {
    friend class Epoche;
    friend class EpocheGuard;
    Epoche &epoche;
    DeletionList &deletionList;

    DeletionList &GetDeletionList() const;

public:
    explicit ThreadInfo(Epoche &epoche);

    ThreadInfo(const ThreadInfo &ti) : epoche(ti.epoche), deletionList(ti.deletionList)
    {}

    ~ThreadInfo();

    Epoche &GetEpoche() const;
};

class Epoche {
    friend class ThreadInfo;
    std::atomic<uint64_t> currentEpoche{0};

    tbb::enumerable_thread_specific<DeletionList> deletionLists;

    size_t startGCThreshhold;

public:
    static constexpr size_t thresholdCounterMask = 64 - 1;
    explicit Epoche(size_t startGCThreshhold) : startGCThreshhold(startGCThreshhold)
    {}

    ~Epoche();

    void EnterEpoche(ThreadInfo &epocheInfo);

    void MarkNodeForDeletion(void *n, ThreadInfo &epocheInfo);

    void ExitEpocheAndCleanup(ThreadInfo &info);

    void ShowDeleteRatio();
};

class EpocheGuard {
    ThreadInfo &threadEpocheInfo;

public:
    explicit EpocheGuard(ThreadInfo &threadEpocheInfo) : threadEpocheInfo(threadEpocheInfo)
    {
        threadEpocheInfo.GetEpoche().EnterEpoche(threadEpocheInfo);
    }

    ~EpocheGuard()
    {
        threadEpocheInfo.GetEpoche().ExitEpocheAndCleanup(threadEpocheInfo);
    }
};

class EpocheGuardReadonly {
public:
    explicit EpocheGuardReadonly(ThreadInfo &threadEpocheInfo)
    {
        threadEpocheInfo.GetEpoche().EnterEpoche(threadEpocheInfo);
    }

    ~EpocheGuardReadonly()
    {}
};

inline ThreadInfo::~ThreadInfo()
{
    deletionList.localEpoche.store(std::numeric_limits<uint64_t>::max());
}
}  // namespace ART

#endif  // ART_EPOCHE_H
