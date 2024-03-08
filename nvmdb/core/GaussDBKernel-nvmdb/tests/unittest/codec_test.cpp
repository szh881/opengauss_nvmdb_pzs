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
 * codec_test.cpp
 *
 * IDENTIFICATION
 *   src/gausskernel/storage/nvmdb/core/GaussDBKernel-nvmdb/tests/unittest/codec_test.cpp
 * -------------------------------------------------------------------------
 */
#include <gtest/gtest.h>

#include "codec/nvm_codec.h"

using namespace NVMDB;

class CodecTest : public ::testing::Test {};

TEST_F(CodecTest, BasicTest)
{
    char buffer1[128] = {0};
    char buffer2[128] = {0};

    EncodeUint32(buffer1, 4);
    EncodeUint32(buffer2, 0x44444443);
    ASSERT_LT(memcmp(buffer1, buffer2, sizeof(uint32)), 0);
    ASSERT_EQ(DecodeUint32(buffer1), 4);
    ASSERT_EQ(DecodeUint32(buffer2), 0x44444443);
    EncodeUint32(buffer2, 3);
    ASSERT_GT(memcmp(buffer1, buffer2, sizeof(uint32)), 0);
    ASSERT_EQ(DecodeUint32(buffer2), 3);

    EncodeInt32(buffer1, 4);
    EncodeInt32(buffer2, -4);
    ASSERT_GT(memcmp(buffer1, buffer2, sizeof(uint32)), 0);
    ASSERT_EQ(DecodeInt32(buffer1), 4);
    ASSERT_EQ(DecodeInt32(buffer2), -4);
    EncodeInt32(buffer2, 5);
    ASSERT_LT(memcmp(buffer1, buffer2, sizeof(uint32)), 0);
    ASSERT_EQ(DecodeInt32(buffer2), 5);

    EncodeInt64(buffer1, 1LL << 34);
    EncodeInt64(buffer2, -1LL * (1LL << 34));
    ASSERT_GT(memcmp(buffer1, buffer2, sizeof(int64)), 0);
    ASSERT_EQ(DecodeInt64(buffer1), 1LL << 34);
    ASSERT_EQ(DecodeInt64(buffer2), -1LL * (1LL << 34));
    EncodeInt64(buffer1, -1);
    ASSERT_GT(memcmp(buffer1, buffer2, sizeof(int64)), 0);
    ASSERT_EQ(DecodeInt64(buffer1), -1);

    char data1[128] = "lmx123";
    char data2[128] = "lmx1234";
    EncodeVarchar(buffer1, data1, strlen(data1));
    EncodeVarchar(buffer2, data2, strlen(data2));
    ASSERT_LT(memcmp(buffer1, buffer2, 128), 0);
    DecodeVarchar(buffer1, data2, 128);
    ASSERT_EQ(strcmp(data2, data1), 0);
    memcpy(data2, "lmx234", 7);
    EncodeVarchar(buffer2, data2, strlen(data2));
    ASSERT_LT(memcmp(buffer1, buffer2, 128), 0);
    memcpy(data2, "abcdefghi\0", 10);
    EncodeVarchar(buffer2, data2, strlen(data2));
}
