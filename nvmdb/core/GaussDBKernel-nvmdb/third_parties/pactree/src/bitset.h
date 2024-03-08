/* -------------------------------------------------------------------------
 *
 * bitset.h
 *
 * Portions Copyright (c) 2023 Huawei Technologies Co.,Ltd.
 * Portions Copyright (c) 2019-2021 Virginia Tech
 *
 * IDENTIFICATION
 *	  src/gausskernel/storage/nvmdb/core/GaussDBKernel-nvmdb/third_parties/pactree/src/bitset.h
 *
 * -------------------------------------------------------------------------
 */
#ifndef pactree_BITSET_H
#define pactree_BITSET_H

#include <cstdint>
#include <assert.h>

namespace hydra {

constexpr int INDEX_MOD = 64;

class bitset {
public:
    void clear()
    {
        bits[0] = 0;
        bits[1] = 0;
    }
    void set(int index)
    {
        setBit(index, bits[index / INDEX_MOD]);
    }
    void reset(int index)
    {
        resetBit(index, bits[index / INDEX_MOD]);
    }
    bool test(int index)
    {
        return testBit(index, bits[index / INDEX_MOD]);
    }
    bool operator[](int index)
    {
        return test(index);
    }
    uint64_t to_ulong()
    {
        return bits[0];
    }
    uint64_t to_ulong(int index)
    {
        return bits[index];
    }

    uint8_t count()
    {
        return count_uint64(bits[0]) + count_uint64(bits[1]);
    }
private:
    uint64_t bits[2];
    bool testBit(int pos, uint64_t &bits)
    {
        return (bits & (1UL << (pos))) != 0;
    }
    void setBit(int pos, uint64_t &bits)
    {
        bits |= 1UL << pos;
    }
    void resetBit(int pos, uint64_t &bits)
    {
        bits &= ~(1UL << pos);
    }

    uint8_t count_uint64(uint64_t u)
    {
        int r = 0;
        while (u) {
            r++;
            u &= u - 1;
        }
        return r;
    }
};
};  // namespace hydra

#endif
