/*
 * Copyright (c) 2023 Huawei Technologies Co.,Ltd.
 *
 * openGauss is licensed under Mulan PSL v2.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *
 *          http://license.coscl.org.cn/MulanPSL2
 *
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 * -------------------------------------------------------------------------
 *
 * nvm_global_bitmap.h
 *
 * IDENTIFICATION
 *   src/gausskernel/storage/nvmdb/core/GaussDBKernel-nvmdb/include/heap/nvm_global_bitmap.h
 * -------------------------------------------------------------------------
 */
#ifndef NVMDB_GLOBAL_BITMAP_H
#define NVMDB_GLOBAL_BITMAP_H

#include <atomic>
#include <cstddef>

#include "heap/nvm_vecrange.h"

namespace NVMDB {

class GlobalBitMap {
public:
    constexpr static uint32 BITMAP_UNIT_SIZE = 64;

    GlobalBitMap(size_t _size);

    ~GlobalBitMap();

    uint32 SyncAcquire();

    void SyncRelease(uint32 bit);

    inline uint32 get_highest_bit() const
    {
        Assert(m_highestBit >= 0 && m_highestBit < m_size);
        return m_highestBit;
    }
private:
    uint64 *m_map{nullptr};
    size_t m_size{0};
    std::atomic<uint32> m_startHint{};
    std::atomic<uint32> m_highestBit{};

    inline uint32 AryOffset(uint32 bit) const
    {
        Assert(bit < m_size);
        return bit / BITMAP_UNIT_SIZE;
    }

    inline uint32 BitOffset(uint32 bit) const
    {
        Assert(bit < m_size);
        return bit % BITMAP_UNIT_SIZE;
    }

    uint32 FFZAndSet(uint64 *data);

    void UpdateHint(uint32 arroff, uint32 bit);
};

}  // namespace NVMDB

#endif  // NVMDB_GLOBAL_BITMAP_H