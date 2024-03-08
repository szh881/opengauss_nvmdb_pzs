/* -------------------------------------------------------------------------
 *
 * N48.cpp
 *
 * Portions Copyright (c) 2023 Huawei Technologies Co.,Ltd.
 * Portions Copyright (c) 2019-2021 Virginia Tech
 * Portions Copyright (c) 2014-2015 Florian Scheibner
 *
 * IDENTIFICATION
 *	  src/gausskernel/storage/nvmdb/core/GaussDBKernel-nvmdb/third_parties/pactree/lib/PDL-ART/N48.cpp
 *
 * -------------------------------------------------------------------------
 */
#include <algorithm>
#include <cassert>
#include "N.h"

namespace ART_ROWEX {

pptr<N> N48::GetChildPptrByIndex(const uint8_t k) const
{
    pptr<N> child = children[k].load();
    while (child.isDirty()) {  // DL
        child = children[k].load();
    }
    return child;
}

bool N48::Insert(uint8_t key, pptr<N> n)
{
    return Insert(key, n, true);
}
bool N48::Insert(uint8_t key, pptr<N> n, bool flush)
{
    uint16_t compactCount = static_cast<uint16_t>(countValues >> compactCountOffset);
    if (compactCount == n48ElementCount) {
        return false;
    }
    n.markDirty();  // DL
    children[compactCount].store(n, std::memory_order_release);
    if (flush)
        flushToNVM(reinterpret_cast<char *>(&children[compactCount]), sizeof(pptr<N>));
    childIndex[key].store(compactCount, std::memory_order_release);
    if (flush) {
        flushToNVM(reinterpret_cast<char *>(&childIndex[key]), sizeof(uint8_t));
        smp_wmb();
    }
    uint32_t increaseCountValues = (1 << 16) + 1;
    countValues += increaseCountValues;  // visible point
    if (flush) {
        flushToNVM(reinterpret_cast<char *>(this), L1_CACHE_BYTES);
        smp_wmb();
    }
    n.markClean();  // DL
    children[compactCount].store(n, std::memory_order_release);
    return true;
}

template <class NODE>
void N48::CopyTo(NODE *n) const
{
    for (unsigned i = 0; i < childrenIndexCount; i++) {
        uint8_t index = childIndex[i].load();
        if (index != emptyMarker) {
            n->Insert(i, GetChildPptrByIndex(index), false);
        }
    }
}

void N48::Change(uint8_t key, pptr<N> val)
{
    uint8_t index = childIndex[key].load();
    assert(index != emptyMarker);
    val.markDirty();  // DL
    children[index].store(val, std::memory_order_release);
    flushToNVM(reinterpret_cast<char *>(&children[index]), sizeof(pptr<N>));
    smp_wmb();
    val.markClean();  // DL
    children[index].store(val, std::memory_order_release);
    return;
}

pptr<N> N48::GetChildPptr(const uint8_t k) const
{
    uint8_t index = childIndex[k].load();
    if (index == emptyMarker) {
        pptr<N> nullPtr(0, 0);
        return nullPtr;
    } else {
        return GetChildPptrByIndex(index);
    }
}

N *N48::GetChild(const uint8_t k) const
{
    uint8_t index = childIndex[k].load();
    if (index == emptyMarker) {
        return nullptr;
    } else {
        pptr<N> child = GetChildPptrByIndex(index);
        N *rawChild = child.getVaddr();
        return rawChild;
    }
}

bool N48::Remove(uint8_t k, bool force)
{
    uint16_t count = static_cast<uint16_t>(countValues);
    if (count == smallestCount && !force) {
        return false;
    }
    assert(childIndex[k] != emptyMarker);
    pptr<N> nullPtr(0, 0);
    nullPtr.markDirty();
    children[childIndex[k]].store(nullPtr, std::memory_order_release);
    flushToNVM(reinterpret_cast<char *>(&children[childIndex[k]]), sizeof(pptr<N>));
    childIndex[k].store(emptyMarker, std::memory_order_release);
    flushToNVM(reinterpret_cast<char *>(&childIndex[k]), sizeof(uint8_t));
    smp_wmb();
    countValues -= 1;  // visible point
    flushToNVM(reinterpret_cast<char *>(this), L1_CACHE_BYTES);
    smp_wmb();
    assert(GetChild(k) == nullptr);
    nullPtr.markClean();
    children[childIndex[k]].store(nullPtr, std::memory_order_release);
    return true;
}

N *N48::GetAnyChild() const
{
    N *anyChild = nullptr;
    for (unsigned i = 0; i < n48ElementCount; i++) {
        pptr<N> child = GetChildPptrByIndex(i);
        N *rawChild = child.getVaddr();
        if (rawChild != nullptr) {
            if (N::IsLeaf(rawChild)) {
                return rawChild;
            }
            anyChild = rawChild;
        }
    }
    return anyChild;
}

N *N48::GetAnyChildReverse() const
{
    N *anyChild = nullptr;
    for (int i = 47; i >= 0; i--) {
        pptr<N> child = GetChildPptrByIndex(i);
        N *rawChild = child.getVaddr();
        if (rawChild != nullptr) {
            if (N::IsLeaf(rawChild)) {
                return rawChild;
            }
            anyChild = rawChild;
        }
    }
    return anyChild;
}

void N48::DeleteChildren()
{
    for (unsigned i = 0; i < childrenIndexCount; i++) {
        if (childIndex[i] != emptyMarker) {
            pptr<N> child = GetChildPptrByIndex(i);
            N *rawChild = child.getVaddr();
            N::DeleteChildren(rawChild);
            N::DeleteNode(rawChild);
        }
    }
}

void N48::GetChildren(uint8_t start, uint8_t end, std::tuple<uint8_t, N *> *&children, uint32_t &childrenCount) const
{
    childrenCount = 0;
    for (unsigned i = start; i <= end; i++) {
        uint8_t index = this->childIndex[i].load();
        if (index != emptyMarker) {
            pptr<N> child = this->children[index].load();
            while (child.isDirty()) {  // DL
                child = this->children[index].load();
            }
            N *rawChild = child.getVaddr();
            if (rawChild != nullptr) {
                children[childrenCount] = std::make_tuple(i, rawChild);
                childrenCount++;
            }
        }
    }
}

N *N48::GetSmallestChild(uint8_t start) const
{
    N *smallestChild = nullptr;
    for (int i = start; i < childrenIndexCount; ++i) {
        uint8_t index = this->childIndex[i].load();
        if (index != emptyMarker) {
            pptr<N> child = children[index].load();
            while (child.isDirty()) {  // DL
                child = children[index].load();
            }
            N *rawChild = child.getVaddr();
            if (rawChild != nullptr) {
                return rawChild;
            }
        }
    }
    return smallestChild;
}
N *N48::GetLargestChild(uint8_t end) const
{
    N *largestChild = nullptr;
    for (int i = end; i >= 0; --i) {
        uint8_t index = this->childIndex[i].load();
        if (index != emptyMarker) {
            pptr<N> child = children[index].load();
            while (child.isDirty()) {  // DL
                child = children[index].load();
            }
            N *rawChild = child.getVaddr();
            if (rawChild != nullptr) {
                return rawChild;
            }
        }
    }
    return largestChild;
}
}  // namespace ART_ROWEX
