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
 * block.h
 *
 * IDENTIFICATION
 *   src/gausskernel/storage/nvmdb/core/GaussDBKernel-nvmdb/include/block.h
 * -------------------------------------------------------------------------
 */
#ifndef _NVMDB_BLOCK_H
#define _NVMDB_BLOCK_H

#include "nvm_types.h"
#include "nvm_utils.h"
#include "nvm_page_dlist.h"

namespace NVMDB {

enum ExtentSizeType {
    EXTSZ_8K,
    EXTSZ_2M,

    EXTSZ_TYPE_NUM,
};

struct ExtentSizeInfo {
    uint32 block_count;
};

static constexpr ExtentSizeInfo gBlockSizeInfo[] = {{1}, {256}};

const uint32 NVM_BLCKSZ = 8192;
const uint32 NVMInvalidBlockNumber = 0;

static inline uint32 GetExtentBlockCount(int extType)
{
    return gBlockSizeInfo[extType].block_count;
}

static inline uint32 GetExtentSize(int extType)
{
    return GetExtentBlockCount(extType) * NVM_BLCKSZ;
}

static inline PointerOffset BlockNumberToOffset(uint32 blkno)
{
    return (PointerOffset)(1LLU * blkno * NVM_BLCKSZ);
}

static inline bool NVMBlockNumberIsValid(uint32 blkno)
{
    return blkno != NVMInvalidBlockNumber;
}

static inline bool NVMBlockNumberIsInvalid(uint32 blkno)
{
    return ((blkno) == NVMInvalidBlockNumber);
}

typedef char *Page;

typedef struct NVMPageHeader {
    PageDListNode m_blkList;
    uint8 m_blksz;
    uint32 m_blkno;
} NVMPageHeader;

#define PageSegmentDListOffset 0

#define PageHeaderSize (sizeof(NVMPageHeader))

static inline uint32 PageContentSize(int extType)
{
    return GetExtentSize(extType) - PageHeaderSize;
}

static inline char* PageGetContent(void* page)
{
    return ((char *)(page)) + sizeof(NVMPageHeader);
}

}  // namespace NVMDB

#endif