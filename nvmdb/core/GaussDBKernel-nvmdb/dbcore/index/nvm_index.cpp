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
 * nvm_index.cpp
 *
 * IDENTIFICATION
 *   src/gausskernel/storage/nvmdb/core/GaussDBKernel-nvmdb/dbcore/index/nvm_index.cpp
 * -------------------------------------------------------------------------
 */
#include "index/nvm_index.h"

namespace NVMDB {

IndexColumnDesc *IndexDescCreate(uint32 colCount)
{
    Assert(colCount != 0);
    return new IndexColumnDesc[colCount];
}

void IndexDescDelete(IndexColumnDesc *desc)
{
    delete[] desc;
}

void InitIndexDesc(IndexColumnDesc *const indexDes, const ColumnDesc *const rowDes, const uint32 &colCnt,
                   uint64 &indexLen)
{
    Assert(indexDes != nullptr && rowDes != nullptr && colCnt != 0);
    uint32 colId = 0;
    uint64 colLen = 0;
    uint64 offset = 0;
    for (uint32 i = 0; i < colCnt; i++) {
        colId = indexDes[i].m_colId;
        colLen = rowDes[colId].m_colLen;
        indexDes[i].m_colLen = colLen;
        indexDes[i].m_colOffset = offset;
        offset += colLen;
    }
    indexLen = offset;
}

}  // namespace NVMDB
