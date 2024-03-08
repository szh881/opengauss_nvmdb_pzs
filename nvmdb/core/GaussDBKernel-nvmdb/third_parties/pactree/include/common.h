/* -------------------------------------------------------------------------
 *
 * common.h
 *
 * Portions Copyright (c) 2023 Huawei Technologies Co.,Ltd.
 * Portions Copyright (c) 2019-2021 Virginia Tech
 *
 * IDENTIFICATION
 *	  src/gausskernel/storage/nvmdb/core/GaussDBKernel-nvmdb/third_parties/pactree/include/common.h
 *
 * -------------------------------------------------------------------------
 */
#ifndef PACTREE_COMMON_H
#define PACTREE_COMMON_H

#include <cstdint>
#include <string>
#include <cstring>
#include <cassert>
#include <iostream>
#include <libpmemobj.h>
#include <atomic>
#include "securec.h"

#define KEYLENGTH 63

#ifndef likely
#define likely(x) __builtin_expect((unsigned long)(x), 1)
#endif
#ifndef unlikely
#define unlikely(x) __builtin_expect((unsigned long)(x), 0)
#endif

#ifndef Assert
#ifdef NDEBUG
#define Assert(p)
#else
#define Assert(p) assert(p)
#endif
#endif

static inline void ALWAYS_CHECK(bool a)
{
    if (!a)
        abort();
}

inline void PacTreeSecureRetCheck(errno_t ret)
{
    if (unlikely(ret != EOK)) {
        abort();
    }
}

template <std::size_t keySize>
class StringKey {
public:
    unsigned char keyLength = 0;
    unsigned char data[keySize];

    StringKey()
    {
        int ret = memset_s(data, keySize, 0, keySize);
        PacTreeSecureRetCheck(ret);
    }

    StringKey(const StringKey &other)
    {
        keyLength = other.keyLength;
        ALWAYS_CHECK(keySize == 0 || keySize >= keyLength);
        if (keyLength != 0) {
            int ret = memcpy_s(data, keyLength, other.data, keyLength);
            PacTreeSecureRetCheck(ret);
        }
    }

    StringKey(const char bytes[])
    {
        keyLength = strlen(bytes);
        ALWAYS_CHECK(keySize == 0 || keySize >= keyLength);
        set(bytes, keyLength);
    }

    StringKey(int k)
    {
        setFromString(std::to_string(k));
    }

    inline StringKey &operator=(const StringKey &other)
    {
        keyLength = other.keyLength;
        ALWAYS_CHECK(keySize == 0 || keySize >= keyLength);
        if (keyLength != 0) {
            int ret = memcpy_s(data, keyLength, other.data, keyLength);
            PacTreeSecureRetCheck(ret);
        }
        return *this;
    }

    inline bool operator<(const StringKey<keySize> &other)
    {
        int len = std::min(size(), other.size());
        int cmp = memcmp(data, other.data, len);
        if (cmp == 0) {
            return size() < other.size();
        } else {
            return cmp < 0;
        }
    }

    inline bool operator>(const StringKey<keySize> &other)
    {
        int len = std::min(size(), other.size());
        int cmp = memcmp(data, other.data, len);
        if (cmp == 0) {
            return size() > other.size();
        } else {
            return cmp > 0;
        }
    }

    inline bool operator==(const StringKey<keySize> &other)
    {
        if (size() != other.size()) {
            return false;
        }
        return memcmp(data, other.data, size()) == 0;
    }

    inline bool operator!=(const StringKey<keySize> &other)
    {
        return !(*this == other);
    }

    inline bool operator<=(const StringKey<keySize> &other)
    {
        return !(*this > other);
    }

    inline bool operator>=(const StringKey<keySize> &other)
    {
        return !(*this < other);
    }

    size_t size() const
    {
        return keyLength;
    }

    inline void setFromString(std::string key)
    {
        keyLength = key.size();
        ALWAYS_CHECK(keySize == 0 || keySize >= keyLength);
        if (keyLength != 0) {
            int ret = memcpy_s(data, keyLength, key.c_str(), keyLength);
            PacTreeSecureRetCheck(ret);
        }
    }

    inline void set(const char bytes[], const std::size_t length)
    {
        keyLength = length;
        ALWAYS_CHECK(keySize == 0 || keySize >= keyLength);
        if (keyLength != 0) {
            int ret = memcpy_s(data, keyLength, bytes, keyLength);
            PacTreeSecureRetCheck(ret);
        }
    }

    char *getData()
    {
        return (char *)data;
    }

    void NextKey()
    {
        if (keyLength > 0) {
            int i = keyLength - 1;
            for (; i >= 0; i--) {
                data[i]++;
                if (data[i] != 0) {
                    break;
                }
            }

            assert(i >= 0);
        }
    }
private:
};

#define STRINGKEY
#ifdef STRINGKEY
using Key_t = StringKey<KEYLENGTH>;
#else
using Key_t = uint64_t;
#endif
using Val_t = uint64_t;
using VarLenString = StringKey<0>;

static const int OpListNodeSize = 2152;

class OpStruct {
public:
    enum Operation { dummy, insert, remove, done };
    enum Step { initial, during_split, finish_split };
    Operation op;                       // 4
    uint16_t poolId;                    // 2
    uint8_t hash;                       // 1
    Step step;                          // 4
    std::atomic<uint8_t> searchLayers;  // 1
    Key_t key;                          //  8
    void *oldNodePtr;                   // old node_ptr 8
    PMEMoid newNodeOid;                 // new node_ptr 16
    Key_t newKey;                       // new key ; 8
    Val_t newVal;                       // new value ; 8
    uint64_t ts;                        // 8
    char oldNodeData[OpListNodeSize];
    bool operator<(const OpStruct &ops) const
    {
        return (ts < ops.ts);
    }
};

#endif
