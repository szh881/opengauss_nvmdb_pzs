/* -------------------------------------------------------------------------
 *
 * linkedList.h
 *
 * Portions Copyright (c) 2023 Huawei Technologies Co.,Ltd.
 * Portions Copyright (c) 2019-2021 Virginia Tech
 *
 * IDENTIFICATION
 *	  src/gausskernel/storage/nvmdb/core/GaussDBKernel-nvmdb/third_parties/pactree/src/linkedList.h
 *
 * -------------------------------------------------------------------------
 */
#ifndef LINKEDLIST_H
#define LINKEDLIST_H
#include "lNode.h"
#include "pactreeTrx.h"

namespace NVMDB {

enum class Operation { LT, GT };

class LinkedList {
public:
    uint64_t genId;
    ListNode *Initialize();
#ifdef SYNC
    bool Insert(Key_t key, Val_t value, ListNode *head, void **locked_parent_node);
#endif
    bool Insert(Key_t &key, Val_t value, ListNode *head, int threadId);
    bool Update(Key_t &key, Val_t value, ListNode *head);
    bool Remove(Key_t &key, ListNode *head);
    bool Probe(Key_t &key, ListNode *head);
    bool Lookup(Key_t &key, Val_t &value, ListNode *head);
    bool ScanInOrder(Key_t &startKey, Key_t &endKey, ListNode *head, int maxRange, LookupSnapshot snapshot,
                     std::vector<std::pair<Key_t, Val_t>> &result);
    static void Print(ListNode *head);
    static uint32_t Size(ListNode *head);
    ListNode *GetHead();
    void Recover(void *sl);
private:
    pptr<ListNode> headPtr;
    pptr<ListNode> tailPtr;
    OpStruct oplogs[100000];  // 500 *200
    int nunOpLogs[112];
};
void PrintDists();

}  // namespace NVMDB

#endif  // _LINKEDLIST_H
