/* -------------------------------------------------------------------------
 *
 * VersionedLock.h
 *
 * Portions Copyright (c) 2023 Huawei Technologies Co.,Ltd.
 * Portions Copyright (c) 2019-2021 Virginia Tech
 *
 * IDENTIFICATION
 *	  src/gausskernel/storage/nvmdb/core/GaussDBKernel-nvmdb/third_parties/pactree/src/VersionedLock.h
 *
 * -------------------------------------------------------------------------
 */
#ifndef VERSIONEDLOCK_H
#define VERSIONEDLOCK_H
#include <atomic>
#include <iostream>
#include "arch.h"

#define PAUSE asm volatile("pause\n" : : : "memory")
using version_t = unsigned long;

constexpr int DEFAULT_VERSION = 2;
constexpr int UINT_OFFSET = 32;

class VersionedLock {
public:
    VersionedLock() : version(DEFAULT_VERSION)
    {}

    version_t ReadLock(uint32_t genId)
    {
        version_t ver = version.load(std::memory_order_acquire);
        uint32_t lockGenId;
        uint32_t verLock;

        lockGenId = ver >> UINT_OFFSET;
        verLock = static_cast<int32_t>(ver);

        if (genId != lockGenId) {
            version_t newVer = (static_cast<uint64_t>(genId) << UINT_OFFSET) + 2;
            if (version.compare_exchange_weak(ver, newVer)) {
                return newVer;
            }
            return 0;
        } else {
            if ((ver & 1) != 0) {
                return 0;
            }
            return ver;
        }
    }

    version_t WriteLock(uint32_t genId)
    {
        version_t ver = version.load(std::memory_order_acquire);
        uint32_t lockGenId;
        uint32_t verLock;

        lockGenId = ver >> UINT_OFFSET;
        verLock = static_cast<int32_t>(ver);

        if (genId != lockGenId) {
            version_t newVer = (static_cast<uint64_t>(genId) << UINT_OFFSET) + 3;
            if (version.compare_exchange_weak(ver, newVer)) {
                return newVer;
            }
            return 0;
        } else {
            if ((ver & 1) == 0 && version.compare_exchange_weak(ver, ver + 1)) {
                return ver;
            }
            return 0;
        }
    }

    bool ReadUnlock(version_t oldVersion)
    {
        std::atomic_thread_fence(std::memory_order_acquire);
        version_t newVersion = version.load(std::memory_order_acquire);
        return newVersion == oldVersion;
    }

    void WriteUnlock()
    {
        version.fetch_add(1, std::memory_order_release);
        return;
    }
private:
    std::atomic<version_t> version;
};
#endif // _VERSIONEDLOCK_H