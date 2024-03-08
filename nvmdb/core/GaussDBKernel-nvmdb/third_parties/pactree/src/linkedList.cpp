/* -------------------------------------------------------------------------
 *
 * linkedList.cpp
 *
 * Portions Copyright (c) 2023 Huawei Technologies Co.,Ltd.
 * Portions Copyright (c) 2019-2021 Virginia Tech
 *
 * IDENTIFICATION
 *	  src/gausskernel/storage/nvmdb/core/GaussDBKernel-nvmdb/third_parties/pactree/src/linkedList.cpp
 *
 * -------------------------------------------------------------------------
 */
#include <climits>
#include <iostream>
#include <cassert>
#include "SearchLayer.h"
#include "Oplog.h"
#include "linkedList.h"
#include "nvm_cfg.h"

namespace NVMDB {

ListNode *LinkedList::Initialize()
{
    genId = 0;
    OpStruct *oplog = (OpStruct *)PMem::getOpLog(0);

    PMem::alloc(LIST_NODE_SIZE, (void **)&headPtr, &(oplog->newNodeOid));
    ListNode *head = (ListNode *)new (headPtr.getVaddr()) ListNode();
    flushToNVM((char *)&headPtr, sizeof(pptr<ListNode>));
    smp_wmb();

    OpStruct *oplog2 = (OpStruct *)PMem::getOpLog(1);

    PMem::alloc(LIST_NODE_SIZE, (void **)&tailPtr, &(oplog->newNodeOid));
    ListNode *tail = (ListNode *)new (tailPtr.getVaddr()) ListNode();

    oplog->op = OpStruct::done;
    oplog2->op = OpStruct::done;

    pptr<ListNode> nullPtr(0, 0);

    head->SetNext(tailPtr);
    head->SetPrev(nullPtr);
    head->SetCur(headPtr);
#ifdef STRINGKEY
    std::string minString = "\0";
    std::string maxString = "~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~";
    Key_t max;
    max.setFromString(maxString);
    Key_t min;
    min.setFromString(minString);
    head->SetMin(min);
    head->SetMax(max);
    head->Insert(min, INVALID_CSN, 0);
    tail->SetMin(max);
    tail->SetMax(max);
    tail->Insert(max, INVALID_CSN, 0);
#else
    head->insert(0, 0, 0);
    head->setMin(0);
    head->setMax(ULLONG_MAX);
    tail->insert(ULLONG_MAX, 0, 0);
    tail->setMin(ULLONG_MAX);
#endif

    tail->SetNext(nullPtr);
    tail->SetPrev(headPtr);
    tail->SetCur(tailPtr);

    flushToNVM(reinterpret_cast<char *>(head), sizeof(ListNode));
    flushToNVM(reinterpret_cast<char *>(tail), sizeof(ListNode));
    smp_wmb();
    return head;
}

static pptr<ListNode> getPrevActiveNode(ListNode *prev)
{
    pptr<ListNode> res;
    while (prev->GetDeleted()) {
        res = prev->GetPrevPtr();
        prev = res.getVaddr();
    }
    return res;
}

static pptr<ListNode> getNextActiveNode(ListNode *next)
{
    pptr<ListNode> res;
    while (next->GetDeleted()) {
        res = next->GetNextPtr();
        next = res.getVaddr();
    }
    return res;
}

static ListNode *searchAndLockNode(ListNode *cur, int genId, Key_t &key)
{
    while (true) {
        cur->SpinWriteLock(genId);
        if (cur->GetMin() > key) {
            ListNode *prev = cur->GetPrev();
            if (cur->GetDeleted() && prev->GetDeleted()) {
                auto prevPtr = getPrevActiveNode(prev);
                cur->SetPrev(prevPtr);
                prev = prevPtr.getVaddr();
            }
            cur->WriteUnlock();
            cur = prev;
            continue;
        }
        if (cur->GetMax() <= key) {
            ListNode *next = cur->GetNext();
            if (cur->GetDeleted() && next->GetDeleted()) {
                auto nextPtr = getNextActiveNode(next);
                cur->SetNext(nextPtr);
                next = nextPtr.getVaddr();
            }
            cur->WriteUnlock();
            cur = next;
            continue;
        }
        break;
    }
    return cur;
}

/*
 * return: true(更新成功）， false(传入的head有问题，需要重新遍历 art）
 */
bool LinkedList::Insert(Key_t &key, Val_t value, ListNode *head, int threadId)
{
    ListNode *cur = head;
    cur = searchAndLockNode(cur, genId, key);

    /* current node is locked and the range is matched */
    bool res = cur->Insert(key, value, 0);
    cur->WriteUnlock();
    return res;
}

bool LinkedList::Lookup(Key_t &key, Val_t &value, ListNode *head)
{
restart:
    ListNode *cur = head;
    while (true) {
        if (cur->GetMin() > key) {
            cur = cur->GetPrev();
            continue;
        }
        if (!cur->CheckRange(key)) {
            cur = cur->GetNext();
            continue;
        }
        break;
    }
    version_t readVersion = cur->ReadLock(genId);
    // Concurrent Update
    if (!readVersion || !cur->CheckRange(key)) {
        goto restart;
}

    bool ret = cur->Lookup(key, value);
    if (!cur->ReadUnlock(readVersion)) {
        goto restart;
}
    return ret;
}

void LinkedList::Print(ListNode *head)
{
    ListNode *cur = head;
    while (cur->GetNext() != nullptr) {
        cur->Print();
        cur = cur->GetNext();
    }
    std::cout << "\n";
}

uint32_t LinkedList::Size(ListNode *head)
{
    ListNode *cur = head;
    int count = 0;
    while (cur->GetNext() != nullptr) {
        count++;
        cur = cur->GetNext();
    }
    return count;
}

ListNode *LinkedList::GetHead()
{
    ListNode *head = (ListNode *)headPtr.getVaddr();
    return head;
}

bool LinkedList::ScanInOrder(Key_t &startKey, Key_t &endKey, ListNode *head, int maxRange, LookupSnapshot snapshot,
                             std::vector<std::pair<Key_t, Val_t>> &result)
{
    ListNode *cur = head;
    cur = searchAndLockNode(cur, genId, startKey);

    // The current node is locked and the range is matched
    //  i.e.,    min <= startKey < max
    assert(!cur->GetDeleted() && cur->GetMin() <= startKey && startKey < cur->GetMax());
    result.clear();
    bool continueScan = false;
    while (true) {
        bool needPrune;
        bool end = cur->ScanInOrder(startKey, endKey, maxRange, snapshot, result, continueScan, &needPrune);
        if (needPrune) {
            cur->Prune(snapshot, genId);
        }
        end |= endKey < cur->GetMax();
        cur->WriteUnlock();
        if (end) {
            break;
        } else {
            ListNode *next = cur->GetNext();
            next->SpinWriteLock(genId);
            cur = next;
            continueScan = true;
        }
    }

    return true;
}

void LinkedList::Recover(void *sl)
{
    genId++;
    auto art = (SearchLayer *)sl;

    for (int i = 0; i < NVMDB_NUM_LOGS_PER_THREAD * NVMDB_MAX_THREAD_NUM; i++) {
        auto oplog = (OpStruct *)PMem::getOpLog(i);
        if (oplog->op == OpStruct::done || oplog->op == OpStruct::dummy) {
            continue;
        }
        if (!(oplog->searchLayers.load() & art->grpMask)) {
            if (oplog->searchLayers.load() == 0) {
                oplog->op = OpStruct::done;
            }
            continue;
        }
        auto remain = oplog->searchLayers.fetch_sub(art->grpMask);

        if (oplog->op == OpStruct::insert) {
            pptr<ListNode> node;
            pptr<ListNode> next(oplog->poolId, oplog->newNodeOid.off);
            node.setRawPtr(oplog->oldNodePtr);
            if (oplog->step == OpStruct::initial) {
                // nothing to do
            } else if (oplog->step == OpStruct::during_split) {
                if (remain == art->grpMask) {
                    /* the split is not finished, just recover the old node; the insert operation considered as failed
                     * not that only one thread need do the recovery
                     * */
                    node.getVaddr()->RecoverSplit(oplog);
                }
            } else {
                // todo: update next's prev point if necessary
                ALWAYS_CHECK(oplog->step == OpStruct::finish_split);
                if (!next.getVaddr()->GetDeleted() &&
                    (art->IsEmpty() || art->lookup(oplog->key) != (void *)next.getRawPtr())) {
                    art->Insert(oplog->key, (void *)next.getRawPtr());
                }
            }
        } else if (oplog->op == OpStruct::remove) {
            if (art->lookup(oplog->key) != nullptr) {
                art->remove(oplog->key, oplog->oldNodePtr);
            }
        }
        if (remain == art->grpMask) {
            oplog->op = OpStruct::done;
        }
    }
}

}  // namespace NVMDB
