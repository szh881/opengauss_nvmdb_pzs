/* -------------------------------------------------------------------------
 *
 * Key.h
 *
 * Portions Copyright (c) 2023 Huawei Technologies Co.,Ltd.
 * Portions Copyright (c) 2019-2021 Virginia Tech
 * Portions Copyright (c) 2014-2015 Florian Scheibner
 *
 * IDENTIFICATION
 *	  src/gausskernel/storage/nvmdb/core/GaussDBKernel-nvmdb/third_parties/pactree/lib/PDL-ART/Key.h
 *
 * -------------------------------------------------------------------------
 */
#ifndef ART_KEY_H
#define ART_KEY_H

#include <cassert>
#include <cstdint>
#include <cstring>
#include <memory>
#include <securec.h>
#include "common.h"

using KeyLen = uint32_t;

class Key {
public:
    static constexpr uint32_t stackLen = 128;
    static constexpr uint32_t defaultLen = 8;
    uint32_t len = 0;

    uint8_t *data;

    uint8_t stackKey[stackLen];

    explicit Key(uint64_t k)
    {
        SetInt(k);
    }

    void SetInt(uint64_t k)
    {
        data = stackKey;
        len = defaultLen;
        *reinterpret_cast<uint64_t *>(stackKey) = __builtin_bswap64(k);
    }

    Key()
    {}

    ~Key()
    {
        if (len > stackLen) {
            delete[] data;
            data = nullptr;
        }
    }

    Key(const Key &key) = delete;

    Key &operator=(const Key &key) = delete;

    Key(Key &&key)
    {
        len = key.len;
        if (len > stackLen) {
            data = key.data;
            key.data = nullptr;
        } else {
            errno_t ret = memcpy_s(stackKey, key.len, key.stackKey, key.len);
            PacTreeSecureRetCheck(ret);
            data = stackKey;
        }
    }

    Key &operator=(Key &&key)
    {
        if (this == &key) {
            return *this;
        }
        len = key.len;
        if (len > stackLen) {
            delete[] data;
            data = key.data;
            key.data = nullptr;
        } else {
            int ret = memcpy_s(stackKey, stackLen, key.stackKey, key.len);
            PacTreeSecureRetCheck(ret);
            data = stackKey;
        }
        return *this;
    }

    void Set(const char bytes[], const std::size_t length)
    {
        if (len > stackLen) {
            delete[] data;
        }
        if (length <= stackLen) {
            errno_t ret = memcpy_s(stackKey, length, bytes, length);
            PacTreeSecureRetCheck(ret);
            data = stackKey;
        } else {
            data = new uint8_t[length];
            errno_t ret = memcpy_s(data, length, bytes, length);
            PacTreeSecureRetCheck(ret);
        }
        len = length;
    }

    void operator=(const char key[])
    {
        if (len > stackLen) {
            delete[] data;
        }
        len = strlen(key);
        if (len <= stackLen) {
            errno_t ret = memcpy_s(stackKey, len, key, len);
            PacTreeSecureRetCheck(ret);
            data = stackKey;
        } else {
            data = new uint8_t[len];
            errno_t ret = memcpy_s(data, len, key, len);
            PacTreeSecureRetCheck(ret);
        }
    }

    bool operator==(const Key &k) const
    {
        if (k.GetKeyLen() != GetKeyLen()) {
            return false;
        }
        return std::memcmp(&k[0], data, GetKeyLen()) == 0;
    }

    bool operator<(const Key &k) const
    {
        for (uint32_t i = 0; i < std::min(k.GetKeyLen(), GetKeyLen()); ++i) {
            if (data[i] > k[i]) {
                return false;
            } else if (data[i] < k[i]) {
                return true;
            }
        }
        return false;
    }

    uint8_t &operator[](std::size_t i)
    {
        Assert(i < len);
        return data[i];
    }

    const uint8_t &operator[](std::size_t i) const
    {
        Assert(i < len);
        return data[i];
    }

    KeyLen GetKeyLen() const
    {
        return len;
    }

    void SetKeyLen(KeyLen newLen)
    {
        if (len == newLen) {
            return;
        }
        if (len > stackLen) {
            delete[] data;
        }
        len = newLen;
        if (len > stackLen) {
            data = new uint8_t[len];
        } else {
            data = stackKey;
        }
    }
};

#endif  // ART_KEY_H
