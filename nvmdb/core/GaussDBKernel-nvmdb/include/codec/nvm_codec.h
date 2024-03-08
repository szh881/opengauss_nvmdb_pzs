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
 * nvm_codec.h
 *
 * IDENTIFICATION
 *   src/gausskernel/storage/nvmdb/core/GaussDBKernel-nvmdb/include/codec/nvm_codec.h
 * -------------------------------------------------------------------------
 */
#ifndef NVMDB_CODEC_H
#define NVMDB_CODEC_H

#include "nvm_types.h"

namespace NVMDB {

enum CODE_TYPE {
    CODE_ROWID = 1,
    CODE_INT32,
    CODE_UINT32,
    CODE_INT64,
    CODE_UINT64,
    CODE_FLOAT,
    CODE_VARCHAR,
    CODE_INVALID = 255,
};

void EncodeInt32(char *buf, int32 i);

int32 DecodeInt32(char *buf);

void EncodeUint32(char *buf, uint32 u);

uint32 DecodeUint32(char *buf);

void EncodeUint64(char *buf, uint64 u);

uint64 DecodeUint64(char *buf);

void EncodeInt64(char *buf, int64 i);

int64 DecodeInt64(char *buf);

void EncodeVarchar(char *buf, char *data, int len);

void DecodeVarchar(char *buf, char *data, int maxlen);

}  // namespace NVMDB

#endif  // NVMDB_CODEC_H
