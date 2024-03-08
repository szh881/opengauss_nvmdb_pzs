/* -------------------------------------------------------------------------
 *
 * N256.cpp
 *
 * Portions Copyright (c) 2023 Huawei Technologies Co.,Ltd.
 * Portions Copyright (c) 2019-2021 Virginia Tech
 * Portions Copyright (c) 2014-2015 Florian Scheibner
 *
 * IDENTIFICATION
 *	  src/gausskernel/storage/nvmdb/core/GaussDBKernel-nvmdb/third_parties/pactree/lib/PDL-ART/N256.cpp
 *
 * -------------------------------------------------------------------------
 */
#include <algorithm>
#include <cassert>
#include "N.h"

namespace ART_ROWEX {
pptr<N> N256::GetChildPptrByIndex(const uint8_t k) const
{
    pptr<N> child = children[k].load();
    while (child.isDirty()) {  // DL
        child = children[k].load();
    }
    return child;
}

void N256::DeleteChildren()
{
    for (uint64_t i = 0; i < childrenIndexCount; ++i) {
        pptr<N> child = GetChildPptrByIndex(i);
        N *rawChild = child.getVaddr();
        if (rawChild != nullptr) {
            N::DeleteChildren(rawChild);
            N::DeleteNode(rawChild);
        }
    }
}

bool N256::Insert(uint8_t key, pptr<N> val)
{
    return Insert(key, val, true);
}
bool N256::Insert(uint8_t key, pptr<N> val, bool flush)
{
    val.markDirty();  // DL
    children[key].store(val, std::memory_order_release);
    if (flush) {
        flushToNVM(reinterpret_cast<char *>(&children[key]), sizeof(pptr<N>));
        smp_wmb();
    }
    uint32_t increaseCountValues = (1 << 16);
    countValues += increaseCountValues + 1;  // visible point
    if (flush) {
        flushToNVM(reinterpret_cast<char *>(this), L1_CACHE_BYTES);
        smp_wmb();
    }
    val.markClean();  // DL
    children[key].store(val, std::memory_order_release);
    return true;
}

template <class NODE>
void N256::CopyTo(NODE *n) const
{
    for (int i = 0; i < childrenIndexCount; ++i) {
        pptr<N> child = GetChildPptrByIndex(i);
        N *rawChild = child.getVaddr();
        if (rawChild != nullptr) {
            n->Insert(i, child, false);
        }
    }
}

void N256::Change(uint8_t key, pptr<N> n)
{
    n.markDirty();  // DL
    children[key].store(n, std::memory_order_release);
    flushToNVM(reinterpret_cast<char *>(&children[key]), sizeof(pptr<N>));
    smp_wmb();
    n.markClean();  // DL
    children[key].store(n, std::memory_order_release);
}

pptr<N> N256::GetChildPptr(const uint8_t k) const
{
    pptr<N> child = children[k].load();
    while (child.isDirty()) {  // DL
        child = children[k].load();
    }
    return child;
}

N *N256::GetChild(const uint8_t k) const
{
    pptr<N> child = children[k].load();
    while (child.isDirty()) {  // DL
        child = children[k].load();
    }
    N *rawChild = child.getVaddr();
    return rawChild;
}

bool N256::Remove(uint8_t k, bool force)
{
    uint16_t count = static_cast<uint16_t>(countValues);
    if (count == smallestCount - 1 && !force) {
        return false;
    }
    assert(force || count > smallestCount - 1);

    pptr<N> nullPtr(0, 0);
    nullPtr.markDirty();  // DL
    children[k].store(nullPtr, std::memory_order_release);
    flushToNVM(reinterpret_cast<char *>(&children[k]), sizeof(pptr<N>));
    smp_wmb();
    countValues -= 1;  // visible point
    flushToNVM(reinterpret_cast<char *>(this), L1_CACHE_BYTES);
    smp_wmb();
    nullPtr.markClean();  // DL
    children[k].store(nullPtr, std::memory_order_release);
    return true;
}

N *N256::GetAnyChild() const
{
    N *anyChild = nullptr;
    for (uint64_t i = 0; i < childrenIndexCount; ++i) {
        pptr<N> child = GetChildPptrByIndex(i);
        N *rawChild = child.getVaddr();
        if (rawChild != nullptr) {
            if (N::IsLeaf(rawChild)) {
                return rawChild;
            } else {
                anyChild = rawChild;
            }
        }
    }
    return anyChild;
}

N *N256::GetAnyChildReverse() const
{
    N *anyChild = nullptr;
    for (int i = 255; i >= 0; --i) {
        pptr<N> child = GetChildPptrByIndex(i);
        N *rawChild = child.getVaddr();
        if (rawChild != nullptr) {
            if (N::IsLeaf(rawChild)) {
                return rawChild;
            } else {
                anyChild = rawChild;
            }
        }
    }
    return anyChild;
}

void N256::GetChildren(uint8_t start, uint8_t end, std::tuple<uint8_t, N *> *&children, uint32_t &childrenCount) const
{
    childrenCount = 0;
    for (unsigned i = start; i <= end; i++) {
        pptr<N> child = GetChildPptrByIndex(i);
        N *rawChild = child.getVaddr();
        if (rawChild != nullptr) {
            children[childrenCount] = std::make_tuple(i, rawChild);
            childrenCount++;
        }
    }
}

N *N256::GetSmallestChild(uint8_t start) const
{
    N *smallestChild = nullptr;
    for (int i = start; i < childrenIndexCount; ++i) {
        pptr<N> child = GetChildPptrByIndex(i);
        N *rawChild = child.getVaddr();
        if (rawChild != nullptr) {
            return rawChild;
        }
    }
    return smallestChild;
}
N *N256::GetLargestChild(uint8_t end) const
{
    N *largestChild = nullptr;
    for (int i = end; i >= 0; --i) {
        pptr<N> child = GetChildPptrByIndex(i);
        while (child.isDirty()) {  // DL
            child = children[i].load();
        }
        N *rawChild = child.getVaddr();
        if (rawChild != nullptr) {
            return rawChild;
        }
    }
    return largestChild;
}
}  // namespace ART_ROWEX
