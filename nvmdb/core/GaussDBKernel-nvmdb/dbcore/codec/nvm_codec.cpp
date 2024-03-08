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
 * nvm_codec.cpp
 *
 * IDENTIFICATION
 *   src/gausskernel/storage/nvmdb/core/GaussDBKernel-nvmdb/dbcore/codec/nvm_codec.cpp
 * -------------------------------------------------------------------------
 */
#include <securec.h>
#include "nvm_types.h"
#include "codec/nvm_codec.h"

namespace NVMDB {

static const uint32 INT32_SIGN = 0x80000000;
static const uint64 INT64_SIGN = 0x8000000000000000;

constexpr int BUF_MASK_LEN = 8;
constexpr int HALF_BUF_MASK_LEN = 4;

/* 把 int32 转为可比较的 uint32 */
uint32 EncodeInt32ToUint32(int32 i)
{
    return uint32(i) ^ INT32_SIGN;
}

int DecodeUint32ToInt32(uint32 u)
{
    return int(u ^ INT32_SIGN);
}

uint64 EncodeInt64ToUint64(int64 i)
{
    return uint64(i) ^ INT64_SIGN;
}

int64 DecodeUint64ToInt64(uint64 u)
{
    return int64(u ^ INT64_SIGN);
}

void EncodeInt32(char *buf, int32 i)
{
    EncodeUint32(buf, EncodeInt32ToUint32(i));
}

int32 DecodeInt32(char *buf)
{
    return DecodeUint32ToInt32(DecodeUint32(buf));
}

void EncodeUint32(char *buf, uint32 u)
{
    for (int i = 3; i >= 0; i--) {
        buf[i] = u & 0xff;
        u = u >> BUF_MASK_LEN;
    }
}

uint32 DecodeUint32(char *buf)
{
    uint32 u = 0;
    for (int i = 0; i < HALF_BUF_MASK_LEN; i++) {
        u = u << BUF_MASK_LEN;
        u |= (unsigned char)buf[i];
    }
    return u;
}

void EncodeUint64(char *buf, uint64 u)
{
    for (int i = 7; i >= 0; i--) {
        buf[i] = u & 0xff;
        u = u >> BUF_MASK_LEN;
    }
}

uint64 DecodeUint64(char *buf)
{
    uint64 u = 0;
    for (int i = 0; i < BUF_MASK_LEN; i++) {
        u = u << BUF_MASK_LEN;
        u |= (unsigned char)buf[i];
    }
    return u;
}

void EncodeInt64(char *buf, int64 i)
{
    EncodeUint64(buf, EncodeInt64ToUint64(i));
}

int64 DecodeInt64(char *buf)
{
    return DecodeUint64ToInt64(DecodeUint64(buf));
}

void EncodeVarchar(char *buf, char *data, int len)
{
    int ret = memcpy_s(buf, len, data, len);
    SecureRetCheck(ret);
    buf[len] = '\0';
}

void DecodeVarchar(char *buf, char *data, int maxlen)
{
    int ret = strcpy_s(buf, maxlen, buf);
    SecureRetCheck(ret);
}

}  // namespace NVMDB