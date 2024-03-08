/* -------------------------------------------------------------------------
 *
 * N.cpp
 *
 * Portions Copyright (c) 2023 Huawei Technologies Co.,Ltd.
 * Portions Copyright (c) 2019-2021 Virginia Tech
 * Portions Copyright (c) 2014-2015 Florian Scheibner
 *
 * IDENTIFICATION
 *	  src/gausskernel/storage/nvmdb/core/GaussDBKernel-nvmdb/third_parties/pactree/lib/PDL-ART/N.cpp
 *
 * -------------------------------------------------------------------------
 */
#include <algorithm>
#include <cassert>
#include <iostream>
#include <new>

#include "N4.hpp"
#include "N16.hpp"
#include "N48.hpp"
#include "N256.hpp"
#include "N.h"

namespace ART_ROWEX {

constexpr int UINT_OFFSET = 63;
constexpr int UINT32_OFFSET = 32;
constexpr int N4_COMPACT_CNT = 4;
constexpr int N16_COMPACT_CNT = 16;
constexpr int N16_DEFAULT_CNT = 14;
constexpr int N48_COMPACT_CNT = 48;

void N::SetType(NTypes type)
{
    typeVersionLockObsolete.fetch_add(ConvertTypeToVersion(type));
}

uint64_t N::ConvertTypeToVersion(NTypes type)
{
    return (static_cast<uint64_t>(type) << (UINT_OFFSET - 1));
}

NTypes N::GetType() const
{
    return static_cast<NTypes>(typeVersionLockObsolete.load(std::memory_order_relaxed) >> (UINT_OFFSET - 1));
}

uint32_t N::GetLevel() const
{
    return level;
}

void N::SetLevel(uint32_t lev)
{
    level = lev;
}

void N::WriteLockOrRestart(bool &needRestart, uint32_t genId)
{
    uint64_t version = typeVersionLockObsolete.load(std::memory_order_relaxed);
    uint64_t newVer = 0;
    uint64_t tmp = 0;
    uint32_t lockGenId;
    uint32_t verLock;

    NTypes type = static_cast<NTypes>(version >> (UINT_OFFSET - 1));

    tmp += static_cast<uint64_t>(static_cast<uint64_t>(type) << (UINT_OFFSET - 1));

    lockGenId = (version - tmp) >> UINT32_OFFSET;
    verLock = static_cast<uint32_t>(version - tmp);
    newVer += tmp;
    newVer += (static_cast<uint64_t>(genId) << UINT32_OFFSET);
    newVer += 0b10;

    if (lockGenId != genId) {
        if (!typeVersionLockObsolete.compare_exchange_weak(version, newVer)) {
            needRestart = true;
            return;
        }
    } else {
        do {
            version = typeVersionLockObsolete.load();
            while (IsLocked(version)) {
                version = typeVersionLockObsolete.load();
            }
            if (IsObsolete(version)) {
                needRestart = true;
                return;
            }
        } while (!typeVersionLockObsolete.compare_exchange_weak(version, version + 0b10));
    }
}

void N::LockVersionOrRestart(uint64_t &version, bool &needRestart, uint32_t genId)
{
    uint64_t newVer = 0;
    uint64_t tmp = 0;
    uint32_t lockGenId;
    uint32_t verLock;

    NTypes type = static_cast<NTypes>(version >> (UINT_OFFSET - 1));

    tmp += static_cast<uint64_t>(static_cast<uint64_t>(type) << (UINT_OFFSET - 1));

    lockGenId = (version - tmp) >> UINT32_OFFSET;
    verLock = static_cast<uint32_t>(version - tmp);
    newVer += tmp;
    newVer += (static_cast<uint64_t>(genId) << UINT32_OFFSET);
    newVer += 0b10;

    if (IsObsolete(version)) {
        needRestart = true;
        return;
    }
    if (lockGenId != genId) {
        if (!typeVersionLockObsolete.compare_exchange_weak(version, newVer)) {
            needRestart = true;
            return;
        } else {
            version = newVer;
        }
    } else {
        if (IsLocked(version)) {
            needRestart = true;
            return;
        }
        if (typeVersionLockObsolete.compare_exchange_strong(version, version + 0b10)) {
            version = version + 0b10;
        } else {
            needRestart = true;
        }
    }
}

void N::WriteUnlock()
{
    assert(IsLocked(typeVersionLockObsolete.load()));
    typeVersionLockObsolete.fetch_sub(0b10);
}

N *N::GetAnyChild(const N *node)
{
    switch (node->GetType()) {
        case NTypes::N4: {
            auto n = static_cast<const N4 *>(node);
            return n->GetAnyChild();
        }
        case NTypes::N16: {
            auto n = static_cast<const N16 *>(node);
            return n->GetAnyChild();
        }
        case NTypes::N48: {
            auto n = static_cast<const N48 *>(node);
            return n->GetAnyChild();
        }
        case NTypes::N256: {
            auto n = static_cast<const N256 *>(node);
            return n->GetAnyChild();
        }
    }
    assert(false);
    __builtin_unreachable();
}

N *N::GetAnyChildReverse(const N *node)
{
    switch (node->GetType()) {
        case NTypes::N4: {
            auto n = static_cast<const N4 *>(node);
            return n->GetAnyChildReverse();
        }
        case NTypes::N16: {
            auto n = static_cast<const N16 *>(node);
            return n->GetAnyChildReverse();
        }
        case NTypes::N48: {
            auto n = static_cast<const N48 *>(node);
            return n->GetAnyChildReverse();
        }
        case NTypes::N256: {
            auto n = static_cast<const N256 *>(node);
            return n->GetAnyChildReverse();
        }
    }
    assert(false);
    __builtin_unreachable();
}

void N::Change(N *node, uint8_t key, pptr<N> val)
{
    switch (node->GetType()) {
        case NTypes::N4: {
            auto n = static_cast<N4 *>(node);
            n->Change(key, val);
            return;
        }
        case NTypes::N16: {
            auto n = static_cast<N16 *>(node);
            n->change(key, val);
            return;
        }
        case NTypes::N48: {
            auto n = static_cast<N48 *>(node);
            n->Change(key, val);
            return;
        }
        case NTypes::N256: {
            auto n = static_cast<N256 *>(node);
            n->Change(key, val);
            return;
        }
    }
    assert(false);
    __builtin_unreachable();
}

template <typename curN, typename biggerN>
void N::InsertGrow(curN *n, N *parentNode, uint8_t keyParent, uint8_t key, pptr<N> val, ThreadInfo &threadInfo,
                   bool &needRestart, OpStruct *oplog, uint32_t genId)
{
    if (n->Insert(key, val)) {
        n->WriteUnlock();
        return;
    }

    pptr<N> nBigPtr;
    oplog->op = OpStruct::insert;

    PMem::alloc(sizeof(biggerN), (void **)&nBigPtr, &(oplog->newNodeOid));
    biggerN *nBig = (biggerN *)new (nBigPtr.getVaddr()) biggerN(n->GetLevel(), n->GetPrefi());

    n->CopyTo(nBig);
    nBig->Insert(key, val);
    ALWAYS_CHECK(nBig->GetCount() >= biggerN::smallestCount);
    flushToNVM(reinterpret_cast<char *>(nBig), sizeof(biggerN));

    parentNode->WriteLockOrRestart(needRestart, genId);
    if (needRestart) {
        PMem::free((void *)nBigPtr.getRawPtr());
        n->WriteUnlock();
        return;
    }

    N::Change(parentNode, keyParent, nBigPtr);
    oplog->op = OpStruct::done;
    parentNode->WriteUnlock();

    n->WriteUnlockObsolete();
    threadInfo.GetEpoche().MarkNodeForDeletion(n, threadInfo);
}

template <typename curN>
void N::InsertCompact(curN *n, N *parentNode, uint8_t keyParent, uint8_t key, pptr<N> val, ThreadInfo &threadInfo,
                      bool &needRestart, OpStruct *oplog, uint32_t genId)
{
    pptr<N> nNewPtr;
    oplog->op = OpStruct::insert;

    PMem::alloc(sizeof(curN), (void **)&nNewPtr, &(oplog->newNodeOid));
    curN *nNew = (curN *)new (nNewPtr.getVaddr()) curN(n->GetLevel(), n->GetPrefi());

    n->CopyTo(nNew);
    nNew->Insert(key, val);
    flushToNVM(reinterpret_cast<char *>(nNew), sizeof(curN));

    parentNode->WriteLockOrRestart(needRestart, genId);
    if (needRestart) {
        PMem::free((void *)nNewPtr.getRawPtr());
        n->WriteUnlock();
        return;
    }

    N::Change(parentNode, keyParent, nNewPtr);
    oplog->op = OpStruct::done;
    parentNode->WriteUnlock();

    n->WriteUnlockObsolete();
    threadInfo.GetEpoche().MarkNodeForDeletion(n, threadInfo);
}

void N::InsertAndUnlock(N *node, N *parentNode, uint8_t keyParent, uint8_t key, pptr<N> val, ThreadInfo &threadInfo,
                        bool &needRestart, OpStruct *oplog, uint32_t genId)
{
    switch (node->GetType()) {
        case NTypes::N4: {
            auto n = static_cast<N4 *>(node);
            uint32_t cValues = n->countValues;
            uint16_t nCompactCount = static_cast<uint16_t>(cValues >> 16);
            uint16_t nCount = static_cast<uint16_t>(cValues);
            if (nCompactCount == N4_COMPACT_CNT && nCount <= (N4_COMPACT_CNT - 1)) {
                InsertCompact<N4>(n, parentNode, keyParent, key, val, threadInfo, needRestart, oplog, genId);
                break;
            }
            InsertGrow<N4, N16>(n, parentNode, keyParent, key, val, threadInfo, needRestart, oplog, genId);
            break;
        }
        case NTypes::N16: {
            auto n = static_cast<N16 *>(node);
            uint32_t cValues = n->countValues;
            uint16_t nCompactCount = static_cast<uint16_t>(cValues >> 16);
            uint16_t nCount = static_cast<uint16_t>(cValues);
            if (nCompactCount == N16_COMPACT_CNT && nCount <= N16_DEFAULT_CNT) {
                InsertCompact<N16>(n, parentNode, keyParent, key, val, threadInfo, needRestart, oplog, genId);
                break;
            }
            InsertGrow<N16, N48>(n, parentNode, keyParent, key, val, threadInfo, needRestart, oplog, genId);
            break;
        }
        case NTypes::N48: {
            auto n = static_cast<N48 *>(node);
            uint32_t cValues = n->countValues;
            uint16_t nCompactCount = static_cast<uint16_t>(cValues >> 16);
            uint16_t nCount = static_cast<uint16_t>(cValues);
            if (nCompactCount == N48_COMPACT_CNT && nCount != N48_COMPACT_CNT) {
                InsertCompact<N48>(n, parentNode, keyParent, key, val, threadInfo, needRestart, oplog, genId);
                break;
            }
            InsertGrow<N48, N256>(n, parentNode, keyParent, key, val, threadInfo, needRestart, oplog, genId);
            break;
        }
        case NTypes::N256: {
            auto n = static_cast<N256 *>(node);
            n->Insert(key, val);
            node->WriteUnlock();
            break;
        }
    }
}

pptr<N> N::GetChildPptr(const uint8_t k, N *node)
{
    switch (node->GetType()) {
        case NTypes::N4: {
            auto n = static_cast<N4 *>(node);
            return n->GetChildPptr(k);
        }
        case NTypes::N16: {
            auto n = static_cast<N16 *>(node);
            return n->getChildPptr(k);
        }
        case NTypes::N48: {
            auto n = static_cast<N48 *>(node);
            return n->GetChildPptr(k);
        }
        case NTypes::N256: {
            auto n = static_cast<N256 *>(node);
            return n->GetChildPptr(k);
        }
    }
    assert(false);
    __builtin_unreachable();
}

N *N::GetChild(const uint8_t k, N *node)
{
    switch (node->GetType()) {
        case NTypes::N4: {
            auto n = static_cast<N4 *>(node);
            return n->GetChild(k);
        }
        case NTypes::N16: {
            auto n = static_cast<N16 *>(node);
            return n->GetChild(k);
        }
        case NTypes::N48: {
            auto n = static_cast<N48 *>(node);
            return n->GetChild(k);
        }
        case NTypes::N256: {
            auto n = static_cast<N256 *>(node);
            return n->GetChild(k);
        }
    }
    assert(false);
    __builtin_unreachable();
}

void N::DeleteChildren(N *node)
{
    if (N::IsLeaf(node)) {
        return;
    }
    switch (node->GetType()) {
        case NTypes::N4: {
            auto n = static_cast<N4 *>(node);
            n->DeleteChildren();
            return;
        }
        case NTypes::N16: {
            auto n = static_cast<N16 *>(node);
            n->DeleteChildren();
            return;
        }
        case NTypes::N48: {
            auto n = static_cast<N48 *>(node);
            n->DeleteChildren();
            return;
        }
        case NTypes::N256: {
            auto n = static_cast<N256 *>(node);
            n->DeleteChildren();
            return;
        }
    }
    assert(false);
    __builtin_unreachable();
}

template <typename curN, typename smallerN>
void N::RemoveAndShrink(curN *n, N *parentNode, uint8_t keyParent, uint8_t key, ThreadInfo &threadInfo,
                        bool &needRestart, OpStruct *oplog, uint32_t genId)
{
    if (n->Remove(key, parentNode == nullptr)) {
        n->WriteUnlock();
        return;
    }

    pptr<N> nSmallPtr;
    oplog->op = OpStruct::insert;
    PMem::alloc(sizeof(smallerN), (void **)&nSmallPtr, &(oplog->newNodeOid));
    smallerN *nSmall = (smallerN *)new (nSmallPtr.getVaddr()) smallerN(n->GetLevel(), n->GetPrefi());

    parentNode->WriteLockOrRestart(needRestart, genId);
    if (needRestart) {
        n->WriteUnlock();
        return;
    }

    n->Remove(key, true);
    n->CopyTo(nSmall);
    flushToNVM(reinterpret_cast<char *>(nSmall), sizeof(smallerN));
    N::Change(parentNode, keyParent, nSmallPtr);
    oplog->op = OpStruct::done;

    parentNode->WriteUnlock();
    n->WriteUnlockObsolete();
    threadInfo.GetEpoche().MarkNodeForDeletion(n, threadInfo);
}

void N::RemoveAndUnlock(N *node, uint8_t key, N *parentNode, uint8_t keyParent, ThreadInfo &threadInfo,
                        bool &needRestart, OpStruct *oplog, uint32_t genId)
{
    switch (node->GetType()) {
        case NTypes::N4: {
            auto n = static_cast<N4 *>(node);
            n->Remove(key, false);
            n->WriteUnlock();
            break;
        }
        case NTypes::N16: {
            auto n = static_cast<N16 *>(node);

            RemoveAndShrink<N16, N4>(n, parentNode, keyParent, key, threadInfo, needRestart, oplog, genId);
            break;
        }
        case NTypes::N48: {
            auto n = static_cast<N48 *>(node);
            RemoveAndShrink<N48, N16>(n, parentNode, keyParent, key, threadInfo, needRestart, oplog, genId);
            break;
        }
        case NTypes::N256: {
            auto n = static_cast<N256 *>(node);
            RemoveAndShrink<N256, N48>(n, parentNode, keyParent, key, threadInfo, needRestart, oplog, genId);
            break;
        }
    }
}

bool N::IsLocked(uint64_t version) const
{
    return ((version & 0b10) == 0b10);
}

uint64_t N::GetVersion() const
{
    return typeVersionLockObsolete.load();
}

bool N::IsObsolete(uint64_t version)
{
    return (version & 1) == 1;
}

bool N::CheckOrRestart(uint64_t startRead) const
{
    return ReadUnlockOrRestart(startRead);
}

bool N::ReadUnlockOrRestart(uint64_t startRead) const
{
    return startRead == typeVersionLockObsolete.load();
}

uint32_t N::GetCount() const
{
    uint32_t cValues = countValues;
    uint16_t c = static_cast<uint16_t>(cValues);
    return c;
}

Prefix N::GetPrefi() const
{
    return prefix.load();
}

void N::SetPrefix(const uint8_t *prefix, uint8_t length, bool flush)
{
    if (length > 0) {
        Prefix p;
        int ret = memcpy_s(p.prefix, std::min(length, MAX_STORED_PREFIX_LENGTH), prefix,
                           std::min(length, MAX_STORED_PREFIX_LENGTH));
        SecureRetCheck(ret);
        p.prefixCount = length;
        this->prefix.store(p, std::memory_order_release);
    } else {
        Prefix p;
        p.prefixCount = 0;
        this->prefix.store(p, std::memory_order_release);
    }
    if (flush) {
        flushToNVM(reinterpret_cast<char *>(&(this->prefix)), sizeof(Prefix));
    }
}

void N::AddPrefixBefore(N *node, uint8_t key)
{
    Prefix p = this->GetPrefi();
    Prefix nodeP = node->GetPrefi();
    uint32_t prefixCopyCount = std::min(static_cast<int>(MAX_STORED_PREFIX_LENGTH), nodeP.prefixCount + 1);
    int ret = memmove_s(p.prefix + prefixCopyCount, MAX_STORED_PREFIX_LENGTH - prefixCopyCount, p.prefix,
                        std::min(static_cast<uint32_t>(p.prefixCount), MAX_STORED_PREFIX_LENGTH - prefixCopyCount));
    PacTreeSecureRetCheck(ret);
    ret = memcpy_s(p.prefix, prefixCopyCount, nodeP.prefix,
                   std::min(prefixCopyCount, static_cast<uint32_t>(nodeP.prefixCount)));
    PacTreeSecureRetCheck(ret);
    if (nodeP.prefixCount < MAX_STORED_PREFIX_LENGTH) {
        p.prefix[prefixCopyCount - 1] = key;
    }
    p.prefixCount += nodeP.prefixCount + 1;
    this->prefix.store(p, std::memory_order_release);
    flushToNVM(reinterpret_cast<char *>(&(this->prefix)), sizeof(Prefix));
}

bool N::IsLeaf(const N *n)
{
    return (reinterpret_cast<uint64_t>(n) & (static_cast<uint64_t>(1) << UINT_OFFSET)) ==
           (static_cast<uint64_t>(1) << UINT_OFFSET);
}

N *N::SetLeaf(TID tid)
{
    return reinterpret_cast<N *>(tid | (static_cast<uint64_t>(1) << UINT_OFFSET));
}

TID N::GetLeaf(const N *n)
{
    return (reinterpret_cast<uint64_t>(n) & ((static_cast<uint64_t>(1) << UINT_OFFSET) - 1));
}

std::tuple<pptr<N>, uint8_t> N::GetSecondChild(N *node, const uint8_t key)
{
    switch (node->GetType()) {
        case NTypes::N4: {
            auto n = static_cast<N4 *>(node);
            return n->GetSecondChild(key);
        }
        default: {
            assert(false);
            __builtin_unreachable();
        }
    }
}

void N::DeleteNode(N *node)
{
    if (N::IsLeaf(node)) {
        return;
    }
    switch (node->GetType()) {
        case NTypes::N4: {
            auto n = static_cast<N4 *>(node);
            PMem::freeVaddr((void *)n);
            return;
        }
        case NTypes::N16: {
            auto n = static_cast<N16 *>(node);
            PMem::freeVaddr((void *)n);
            return;
        }
        case NTypes::N48: {
            auto n = static_cast<N48 *>(node);
            PMem::freeVaddr((void *)n);
            return;
        }
        case NTypes::N256: {
            auto n = static_cast<N256 *>(node);
            PMem::freeVaddr((void *)n);
            return;
        }
    }
    PMem::freeVaddr(node);
}

TID N::GetAnyChildTid(const N *n)
{
    const N *nextNode = n;

    while (true) {
        const N *node = nextNode;
        nextNode = GetAnyChild(node);

        assert(nextNode != nullptr);
        if (IsLeaf(nextNode)) {
            return GetLeaf(nextNode);
        }
    }
}

TID N::GetAnyChildTidReverse(const N *n)
{
    const N *nextNode = n;

    while (true) {
        const N *node = nextNode;
        nextNode = GetAnyChildReverse(node);

        assert(nextNode != nullptr);
        if (IsLeaf(nextNode)) {
            return GetLeaf(nextNode);
        }
    }
}

void N::GetChildren(const N *node, uint8_t start, uint8_t end, std::tuple<uint8_t, N *> children[],
                    uint32_t &childrenCount)
{
    switch (node->GetType()) {
        case NTypes::N4: {
            auto n = static_cast<const N4 *>(node);
            n->GetChildren(start, end, children, childrenCount);
            return;
        }
        case NTypes::N16: {
            auto n = static_cast<const N16 *>(node);
            n->GetChildren(start, end, children, childrenCount);
            return;
        }
        case NTypes::N48: {
            auto n = static_cast<const N48 *>(node);
            n->GetChildren(start, end, children, childrenCount);
            return;
        }
        case NTypes::N256: {
            auto n = static_cast<const N256 *>(node);
            n->GetChildren(start, end, children, childrenCount);
            return;
        }
    }
}
N *N::GetSmallestChild(const N *node, uint8_t start)
{
    switch (node->GetType()) {
        case NTypes::N4: {
            auto n = static_cast<const N4 *>(node);
            return n->GetSmallestChild(start);
        }
        case NTypes::N16: {
            auto n = static_cast<const N16 *>(node);
            return n->GetSmallestChild(start);
        }
        case NTypes::N48: {
            auto n = static_cast<const N48 *>(node);
            return n->GetSmallestChild(start);
        }
        case NTypes::N256: {
            auto n = static_cast<const N256 *>(node);
            return n->GetSmallestChild(start);
        }
    }
    assert(false);
    __builtin_unreachable();
}
N *N::GetLargestChild(const N *node, uint8_t start)
{
    switch (node->GetType()) {
        case NTypes::N4: {
            auto n = static_cast<const N4 *>(node);
            return n->GetLargestChild(start);
        }
        case NTypes::N16: {
            auto n = static_cast<const N16 *>(node);
            return n->GetLargestChild(start);
        }
        case NTypes::N48: {
            auto n = static_cast<const N48 *>(node);
            return n->GetLargestChild(start);
        }
        case NTypes::N256: {
            auto n = static_cast<const N256 *>(node);
            return n->GetLargestChild(start);
        }
    }
    assert(false);
    __builtin_unreachable();
}
}  // namespace ART_ROWEX
