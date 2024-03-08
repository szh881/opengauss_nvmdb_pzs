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
 * nvm_table.cpp
 *
 * IDENTIFICATION
 *   src/gausskernel/storage/nvmdb/core/GaussDBKernel-nvmdb/dbcore/nvm_table.cpp
 * -------------------------------------------------------------------------
 */
#include "nvm_table.h"
#include "nvm_vecstore.h"
#include "nvm_heap_space.h"

namespace NVMDB {

bool TableDescInit(TableDesc *desc, uint32 colCount)
{
    desc->col_cnt = colCount;
    desc->row_len = 0;
    desc->col_desc = new (std::nothrow) ColumnDesc[colCount];

    return desc->col_desc != nullptr;
}

void TableDescDestroy(TableDesc *desc)
{
    desc->col_cnt = 0;
    desc->row_len = 0;
    delete[] desc->col_desc;
}

uint32 Table::CreateSegment()
{
    g_heapSpace->AllocNewExtent(&m_seghead, EXTSZ_2M);
    m_rowidMap = GetRowIdMap(m_seghead, m_rowLen);
    return m_seghead;
}

void Table::Mount(uint32 seghead)
{
    m_seghead = seghead;
    m_rowidMap = GetRowIdMap(seghead, m_rowLen);
}

uint32 Table::GetColIdByName(const char *name) const
{
    uint32 i;
    uint32 max = m_desc.col_cnt;
    if (name == nullptr) {
        return InvalidColId;
    }

    for (i = 0; i < max; i++) {
        if (strcmp(name, m_desc.col_desc[i].m_colName) == 0) {
            break;
        }
    }
    return (i < max ? i : InvalidColId);
}

}  // namespace NVMDB
