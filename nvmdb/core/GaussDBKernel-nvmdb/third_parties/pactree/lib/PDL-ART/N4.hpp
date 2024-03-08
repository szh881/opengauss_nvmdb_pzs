/* -------------------------------------------------------------------------
 *
 * N4.cpp
 *
 * Portions Copyright (c) 2023 Huawei Technologies Co.,Ltd.
 * Portions Copyright (c) 2019-2021 Virginia Tech
 * Portions Copyright (c) 2014-2015 Florian Scheibner
 *
 * IDENTIFICATION
 *	  src/gausskernel/storage/nvmdb/core/GaussDBKernel-nvmdb/third_parties/pactree/lib/PDL-ART/N4.cpp
 *
 * -------------------------------------------------------------------------
 */
#include <algorithm>
#include <cassert>
#include "N.h"

namespace ART_ROWEX {

void N4::DeleteChildren()
{
    uint16_t compactCount = static_cast<uint16_t>(countValues >> compactCountOffset);
    for (uint32_t i = 0; i < compactCount; ++i) {
        pptr<N> child = children[i].load();
        N *rawChild = child.getVaddr();
        if (rawChild != nullptr) {
            N::DeleteChildren(rawChild);
            N::DeleteNode(rawChild);
        }
    }
}

bool N4::Insert(uint8_t key, pptr<N> n)
{
    return Insert(key, n, true);
}

bool N4::Insert(uint8_t key, pptr<N> n, bool flush)
{
    uint16_t compactCount = static_cast<uint16_t>(countValues >> compactCountOffset);
    if (compactCount == n4ElementCount) {
        return false;
    }
    keys[compactCount].store(key, std::memory_order_release);
    n.markDirty();
    children[compactCount].store(n, std::memory_order_release);  // visible point for insert function
    uint32_t increaseCountValues = (1 << compactCountOffset) + 1;
    countValues += increaseCountValues;  // visible point
    if (flush) {
        flushToNVM(reinterpret_cast<char *>(this), sizeof(N4));
        smp_wmb();
    }
    n.markClean();
    children[compactCount].store(n, std::memory_order_release);  // visible point for insert function

    return true;
}

template <class NODE>
void N4::CopyTo(NODE *n) const
{
    uint16_t compactCount = static_cast<uint16_t>(countValues >> compactCountOffset);
    for (uint32_t i = 0; i < compactCount; ++i) {
        pptr<N> child = children[i].load();
        N *rawChild = child.getVaddr();
        if (rawChild != nullptr) {
            n->Insert(keys[i].load(), child, false);  // no flush
        }
    }
}

void N4::Change(uint8_t key, pptr<N> val)
{
    uint16_t compactCount = static_cast<uint16_t>(countValues >> compactCountOffset);
    for (uint32_t i = 0; i < compactCount; ++i) {
        pptr<N> child = children[i].load();
        N *rawChild = child.getVaddr();
        if (rawChild != nullptr && keys[i].load() == key) {
            val.markDirty();
            children[i].store(val, std::memory_order_release);
            flushToNVM((char *)&children[i], sizeof(pptr<N>));
            smp_wmb();
            val.markClean();
            children[i].store(val, std::memory_order_release);
            return;
        }
    }
    assert(false);
    __builtin_unreachable();
}
pptr<N> N4::GetChildPptr(const uint8_t k) const
{
    for (uint32_t i = 0; i < n4ElementCount; ++i) {
        pptr<N> child = children[i].load();
        while (child.isDirty()) {  // DL
            child = children[i].load();
        }
        N *rawChild = child.getVaddr();
        if (rawChild != nullptr && keys[i].load() == k) {
            return child;
        }
    }
    pptr<N> nullPtr(0, 0);
    return nullPtr;
}

N *N4::GetChild(const uint8_t k) const
{
    for (uint32_t i = 0; i < n4ElementCount; ++i) {
        pptr<N> child = children[i].load();
        while (child.isDirty()) {  // DL
            child = children[i].load();
        }
        N *rawChild = child.getVaddr();
        if (rawChild != nullptr && keys[i].load() == k) {
            return rawChild;
        }
    }
    return nullptr;
}

bool N4::Remove(uint8_t k, bool force)
{
    uint16_t compactCount = static_cast<uint16_t>(countValues >> 16);
    for (uint32_t i = 0; i < compactCount; ++i) {
        pptr<N> child = children[i].load();
        N *rawChild = child.getVaddr();
        if (rawChild != nullptr && keys[i].load() == k) {
            countValues -= 1;  // visible point
            pptr<N> nullPtr(0, 0);
            nullPtr.markDirty();
            children[i].store(nullPtr, std::memory_order_release);
            flushToNVM(reinterpret_cast<char *>(&children[i]), sizeof(pptr<N>));
            smp_wmb();
            nullPtr.markClean();
            children[i].store(nullPtr, std::memory_order_release);
            return true;
        }
    }
    assert(false);
    __builtin_unreachable();
}

N *N4::GetAnyChild() const
{
    N *anyChild = nullptr;
    for (uint32_t i = 0; i < n4ElementCount; ++i) {
        pptr<N> child = children[i].load();
        while (child.isDirty()) {  // DL
            child = children[i].load();
        }
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

N *N4::GetAnyChildReverse() const
{
    N *anyChild = nullptr;
    for (int i = 3; i >= 0; --i) {
        pptr<N> child = children[i].load();
        while (child.isDirty()) {  // DL
            child = children[i].load();
        }
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

std::tuple<pptr<N>, uint8_t> N4::GetSecondChild(const uint8_t key) const
{
    uint16_t compactCount = static_cast<uint16_t>(countValues >> 16);
    for (uint32_t i = 0; i < compactCount; ++i) {
        pptr<N> child = children[i].load();
        while (child.isDirty()) {  // DL
            child = children[i].load();
        }
        N *rawChild = child.getVaddr();
        if (rawChild != nullptr) {
            uint8_t k = keys[i].load();
            if (k != key) {
                return std::make_tuple(child, k);
            }
        }
    }
    pptr<N> nullPtr;
    nullPtr.setRawPtr(0);
    return std::make_tuple(nullPtr, 0);
}

void N4::GetChildren(uint8_t start, uint8_t end, std::tuple<uint8_t, N *> *&children, uint32_t &childrenCount) const
{
    childrenCount = 0;
    for (uint32_t i = 0; i < n4ElementCount; ++i) {
        uint8_t key = this->keys[i].load();
        if (key >= start && key <= end) {
            pptr<N> child = this->children[i].load();
            while (child.isDirty()) {  // DL
                child = this->children[i].load();
            }
            N *rawChild = child.getVaddr();
            if (rawChild != nullptr) {
                children[childrenCount] = std::make_tuple(key, rawChild);
                childrenCount++;
            }
        }
    }
    std::sort(children, children + childrenCount,
              [](auto &first, auto &second) { return std::get<0>(first) < std::get<0>(second); });
}
N *N4::GetSmallestChild(uint8_t start) const
{
    N *smallestChild = nullptr;
    uint8_t minKey = 255;
    for (uint32_t i = 0; i < n4ElementCount; ++i) {
        uint8_t key = this->keys[i].load();
        if (key >= start && key <= minKey) {
            pptr<N> child = children[i].load();
            while (child.isDirty()) {  // DL
                child = children[i].load();
            }
            N *rawChild = child.getVaddr();
            if (rawChild != nullptr) {
                minKey = key;
                smallestChild = rawChild;
            }
        }
    }
    return smallestChild;
}
N *N4::GetLargestChild(uint8_t end) const
{
    N *largestChild = nullptr;
    uint8_t maxKey = 0;
    for (uint32_t i = 0; i < n4ElementCount; ++i) {
        uint8_t key = this->keys[i].load();
        if (key <= end && key >= maxKey) {
            pptr<N> child = children[i].load();
            while (child.isDirty()) {  // DL
                child = children[i].load();
            }
            N *rawChild = child.getVaddr();
            if (rawChild != nullptr) {
                maxKey = key;
                largestChild = rawChild;
            }
        }
    }
    return largestChild;
}
}  // namespace ART_ROWEX
