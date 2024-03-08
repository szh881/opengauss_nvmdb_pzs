/* -------------------------------------------------------------------------
 *
 * pptr.h
 *    pptr class definition
 *
 * Portions Copyright (c) 2023 Huawei Technologies Co.,Ltd.
 * Portions Copyright (c) 2019-2021 Virginia Tech
 *
 * IDENTIFICATION
 *	  src/gausskernel/storage/nvmdb/core/GaussDBKernel-nvmdb/third_parties/pactree/include/pptr.h
 *
 * -------------------------------------------------------------------------
 */
#ifndef PACTREE_PPTR_H
#define PACTREE_PPTR_H

#include "pmem.h"

template <typename T>
class pptr {
public:
    static constexpr size_t validAddressBits = 48;
    static constexpr size_t dirtyBitOffset = 61;
    pptr() noexcept : rawPtr{}
    {}
    pptr(int poolId, uintptr_t offset)
    {
        rawPtr = (((static_cast<uintptr_t>(poolId)) << validAddressBits) | offset);
    }
    T *operator->()
    {
        int poolId = (rawPtr & MASK_POOL) >> 48;
        void *baseAddr = PMem::getBaseOf(poolId);
        uintptr_t offset = rawPtr & MASK;
        return reinterpret_cast<T *>(reinterpret_cast<uintptr_t>(baseAddr) + offset);
    }

    T *getVaddr()
    {
        uintptr_t ptr = rawPtr;
        uintptr_t offset = ptr & MASK;
        if (offset == 0) {
            return nullptr;
        }

        int poolId = (int)((ptr & MASK_POOL) >> 48);
        void *baseAddr = PMem::getBaseOf(poolId);
        return reinterpret_cast<T *>(reinterpret_cast<uintptr_t>(baseAddr) + offset);
    }

    uintptr_t getRawPtr()
    {
        return rawPtr;
    }

    void setRawPtr(void *p)
    {
        rawPtr = reinterpret_cast<uintptr_t>(p);
    }

    inline void markDirty()
    {
        rawPtr = ((1UL << dirtyBitOffset) | rawPtr);
    }

    bool isDirty()
    {
        return (((1UL << dirtyBitOffset) & rawPtr) == (1UL << dirtyBitOffset));
    }

    inline void markClean()
    {
        rawPtr = (rawPtr & MASK_DIRTY);
    }

    int getPoolId()
    {
        return (rawPtr & MASK_POOL) >> validAddressBits;
    }

private:
    uintptr_t rawPtr;  // 16b + 48 b // nvm
};

#endif