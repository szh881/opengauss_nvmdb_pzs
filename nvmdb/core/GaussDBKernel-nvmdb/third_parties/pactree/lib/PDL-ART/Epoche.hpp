/* -------------------------------------------------------------------------
 *
 * Epoche.cpp
 *
 * Portions Copyright (c) 2023 Huawei Technologies Co.,Ltd.
 * Portions Copyright (c) 2019-2021 Virginia Tech
 * Portions Copyright (c) 2014-2015 Florian Scheibner
 *
 * IDENTIFICATION
 *	  src/gausskernel/storage/nvmdb/core/GaussDBKernel-nvmdb/third_parties/pactree/lib/PDL-ART/Epoche.cpp
 *
 * -------------------------------------------------------------------------
 */
#ifndef EPOCHE_CPP
#define EPOCHE_CPP

#include <cassert>
#include <iostream>
#include <libpmemobj.h>
#include "Epoche.h"

using namespace ART;

inline DeletionList::~DeletionList()
{
    assert(deletitionListCount == 0 && headDeletionList == nullptr);
    LabelDelete *cur = nullptr;
    LabelDelete *next = freeLabelDeletes;
    while (next != nullptr) {
        cur = next;
        next = cur->next;
        delete cur;
    }
    freeLabelDeletes = nullptr;
}

inline std::size_t DeletionList::Size()
{
    return deletitionListCount;
}

inline void DeletionList::Remove(LabelDelete *label, LabelDelete *prev)
{
    if (prev == nullptr) {
        headDeletionList = label->next;
    } else {
        prev->next = label->next;
    }
    deletitionListCount -= label->nodesCount;

    label->next = freeLabelDeletes;
    freeLabelDeletes = label;
    deleted += label->nodesCount;
}

inline void DeletionList::Add(void *n, uint64_t globalEpoch)
{
    deletitionListCount++;
    LabelDelete *label;
    if (headDeletionList != nullptr && headDeletionList->nodesCount < headDeletionList->nodes.size()) {
        label = headDeletionList;
    } else {
        if (freeLabelDeletes != nullptr) {
            label = freeLabelDeletes;
            freeLabelDeletes = freeLabelDeletes->next;
        } else {
            label = new LabelDelete();
        }
        label->nodesCount = 0;
        label->next = headDeletionList;
        headDeletionList = label;
    }
    label->nodes[label->nodesCount] = n;
    label->nodesCount++;
    label->epoche = globalEpoch;

    added++;
}

inline LabelDelete *DeletionList::Head()
{
    return headDeletionList;
}

inline void Epoche::EnterEpoche(ThreadInfo &epocheInfo)
{
    unsigned long curEpoche = currentEpoche.load(std::memory_order_relaxed);
    epocheInfo.GetDeletionList().localEpoche.store(curEpoche, std::memory_order_release);
}

inline void Epoche::MarkNodeForDeletion(void *n, ThreadInfo &epocheInfo)
{
    epocheInfo.GetDeletionList().Add(n, currentEpoche.load());
    epocheInfo.GetDeletionList().thresholdCounter++;
}

inline void Epoche::ExitEpocheAndCleanup(ThreadInfo &epocheInfo)
{
    DeletionList &deletionList = epocheInfo.GetDeletionList();
    if ((deletionList.thresholdCounter & thresholdCounterMask) == 1) {
        currentEpoche++;
    }
    if (deletionList.thresholdCounter > startGCThreshhold) {
        if (deletionList.Size() == 0) {
            deletionList.thresholdCounter = 0;
            return;
        }
        deletionList.localEpoche.store(std::numeric_limits<uint64_t>::max());

        uint64_t oldestEpoche = std::numeric_limits<uint64_t>::max();
        for (auto &epoche : deletionLists) {
            auto e = epoche.localEpoche.load();
            if (e < oldestEpoche) {
                oldestEpoche = e;
            }
        }

        LabelDelete *cur = deletionList.Head();
        LabelDelete *next = nullptr;
        LabelDelete *prev = nullptr;
        while (cur != nullptr) {
            next = cur->next;

            if (cur->epoche < oldestEpoche) {
                for (std::size_t i = 0; i < cur->nodesCount; ++i) {
                    PMEMoid ptr = pmemobj_oid(cur->nodes[i]);
                    pmemobj_free(&ptr);
                }
                deletionList.Remove(cur, prev);
            } else {
                prev = cur;
            }
            cur = next;
        }
        deletionList.thresholdCounter = 0;
    }
}

inline Epoche::~Epoche()
{
    uint64_t oldestEpoche = std::numeric_limits<uint64_t>::max();
    for (auto &epoche : deletionLists) {
        auto e = epoche.localEpoche.load();
        if (e < oldestEpoche) {
            oldestEpoche = e;
        }
    }
    for (auto &d : deletionLists) {
        LabelDelete *cur = d.Head(), *next, *prev = nullptr;
        while (cur != nullptr) {
            next = cur->next;

            assert(cur->epoche < oldestEpoche);
            for (std::size_t i = 0; i < cur->nodesCount; ++i) {
                PMEMoid ptr = pmemobj_oid(cur->nodes[i]);
                pmemobj_free(&ptr);
            }
            d.Remove(cur, prev);
            cur = next;
        }
    }
}

inline void Epoche::ShowDeleteRatio()
{
    for (auto &d : deletionLists) {
        std::cout << "deleted " << d.deleted << " of " << d.added << std::endl;
    }
}

inline ThreadInfo::ThreadInfo(Epoche &epoche) : epoche(epoche), deletionList(epoche.deletionLists.local())
{}

inline DeletionList &ThreadInfo::GetDeletionList() const
{
    return deletionList;
}

inline Epoche &ThreadInfo::GetEpoche() const
{
    return epoche;
}

#endif  // EPOCHE_CPP
