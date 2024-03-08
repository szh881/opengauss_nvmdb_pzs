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
 * nvm_utils.h
 *
 * IDENTIFICATION
 *   src/gausskernel/storage/nvmdb/core/GaussDBKernel-nvmdb/include/nvm_utils.h
 * -------------------------------------------------------------------------
 */
#ifndef NVMDB_UTILS_H
#define NVMDB_UTILS_H

#include <cassert>
#include <cstdlib>
#include <cstring>
#include "securec.h"

namespace NVMDB {

#define BITMAP_BYTE_IX(x) ((x) >> 3)
#define BITMAP_GETLEN(x) (BITMAP_BYTE_IX(x) + 1)
#define BITMAP_SET(b, x) (b[BITMAP_BYTE_IX(x)] |= (1 << ((x)&0x07)))
#define BITMAP_CLEAR(b, x) (b[BITMAP_BYTE_IX(x)] &= ~(1 << ((x)&0x07)))
#define BITMAP_GET(b, x) (b[BITMAP_BYTE_IX(x)] & (1 << ((x)&0x07)))

#ifndef likely
#define likely(x) __builtin_expect((x) != 0, 1)
#endif
#ifndef unlikely
#define unlikely(x) __builtin_expect((x) != 0, 0)
#endif

constexpr int MAX_SEED_LEN = 3;

inline void SecureRetCheck(errno_t ret)
{
    if (unlikely(ret != EOK)) {
        abort();
    }
}

static inline void ALWAYS_CHECK(bool a)
{
    if (unlikely(!a)) {
        abort();
    }
}

template <typename T>
constexpr T CompileValue(T value __attribute__((unused)), T debugValue __attribute__((unused)))
{
#ifdef NDEBUG
    return value;
#else
    return debugValue;
#endif
}

#ifndef Assert
#ifdef NDEBUG
#define Assert(p)
#else
#define Assert(p) assert(p)
#endif
#endif

/*
 * serve as a hint for compiler -  if cond not satisified, behavior is an assertion
 */
[[noreturn]] inline void not_reachable()
{
    assert(false);
    __builtin_unreachable();
}

#define CM_ALIGN_ANY(size, align) (((size) + (align)-1) / (align) * (align))

class RandomGenerator {
    unsigned short seed[MAX_SEED_LEN];
    unsigned short seed2[MAX_SEED_LEN];
    unsigned short inital[MAX_SEED_LEN];
    unsigned short inital2[MAX_SEED_LEN];

public:
    RandomGenerator()
    {
        for (int i = 0; i < MAX_SEED_LEN; i++) {
            inital[i] = seed[i] = random();
            inital2[i] = seed2[i] = random();
        }
    }
    int randomInt()
    {
        return nrand48(seed) ^ nrand48(seed2);
    }
    double randomDouble()
    {
        return erand48(seed) * erand48(seed2);
    }
    void setSeed(unsigned short newseed[MAX_SEED_LEN])
    {
        constexpr size_t memLen = sizeof(unsigned short) * MAX_SEED_LEN;
        int ret = memcpy_s(seed, memLen, newseed, memLen);
        SecureRetCheck(ret);
    }
    void reset()
    {
        constexpr size_t memLen = sizeof(unsigned short) * MAX_SEED_LEN;
        int ret = memcpy_s(seed, memLen, inital, memLen);
        SecureRetCheck(ret);
        ret = memcpy_s(seed2, memLen, inital2, memLen);
        SecureRetCheck(ret);
    }
    long long Next()
    {
        return randomInt();
    }
} __attribute__((aligned(64)));

}  // namespace NVMDB

#endif // NVMDB_UTILS_H