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
 * nvm_global_bitmap.cpp
 *
 * IDENTIFICATION
 *   src/gausskernel/storage/nvmdb/core/GaussDBKernel-nvmdb/dbcore/heap/nvm_global_bitmap.cpp
 * -------------------------------------------------------------------------
 */
#include <cstring>

#include "nvm_utils.h"
#include "nvm_global_bitmap.h"

namespace NVMDB {

GlobalBitMap::GlobalBitMap(size_t size) : m_size(size)
{
    assert(size >= BITMAP_UNIT_SIZE);
    m_map = new uint64[m_size / BITMAP_UNIT_SIZE];
    size_t memSize = m_size / BITMAP_UNIT_SIZE * sizeof(uint64);
    int ret = memset_s(m_map, memSize, 0, memSize);
    SecureRetCheck(ret);
}

GlobalBitMap::~GlobalBitMap()
{
    delete[] m_map;
}

/* Find first zero in the data and tas it, return the position [0, 63] or 64 meaning no zero */
uint32 GlobalBitMap::FFZAndSet(uint64 *data)
{
    uint64 value = *data;
    while (true) {
        value = ~value;
        /*
         * ctzl 返回右起第一个1，右边的0的个数。
         *    value = .... 1110 1011
         *   ~value = .... 0001 0100
         *   ctzl() = 2, 所以 bit = 2 的是可以用的。
         */
        uint32 fftz = 64;
        if (value != 0) {
            fftz = __builtin_ctzl(value);
        }
        if (fftz != BITMAP_UNIT_SIZE) {
            Assert(fftz >= 0 && fftz < BITMAP_UNIT_SIZE);
            uint64 mask = 1LLU << fftz;
            uint64 oldv = __sync_fetch_and_or(data, mask);
            if (oldv & mask) {
                /* 如果已经被其他人占用了，重试 */
                value = oldv;
                continue;
            }
        }
        return fftz;
    }
}

void GlobalBitMap::UpdateHint(uint32 arroff, uint32 bit)
{
    uint32 oldStart = m_startHint.load();
    while (oldStart < arroff) {
        if (m_startHint.compare_exchange_weak(oldStart, arroff)) {
            break;
        }
    }
    uint32 oldHbit = m_highestBit.load();
    while (oldHbit < bit) {
        if (m_highestBit.compare_exchange_weak(oldHbit, bit)) {
            break;
        }
    }
}

uint32 GlobalBitMap::SyncAcquire()
{
restart:
    uint32 start = m_startHint;
    for (uint32 aryoff = start; aryoff < m_size / BITMAP_UNIT_SIZE; aryoff++) {
        uint32 ffz = FFZAndSet(&m_map[aryoff]);
        if (ffz == BITMAP_UNIT_SIZE) {
            continue;
        }

        uint32 bit = aryoff * BITMAP_UNIT_SIZE + ffz;
        UpdateHint(aryoff, bit);

        return bit;
    }

    /* can't find any unused bit with hinted start off, restart from beginning. */
    if (start != 0) {
        m_startHint = 0;
        goto restart;
    }

    not_reachable();
}

void GlobalBitMap::SyncRelease(uint32 bit)
{
    uint32 aryoff = AryOffset(bit);
    uint64 mask = 1LLU << BitOffset(bit);

    Assert(m_map[aryoff] & mask);
    uint64 oldv = __sync_fetch_and_and(&m_map[aryoff], ~mask);
    Assert(oldv & mask);

    if (aryoff < m_startHint) {
        m_startHint = aryoff;
    }
}

}  // namespace NVMDB
