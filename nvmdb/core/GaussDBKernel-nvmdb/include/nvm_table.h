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
 * nvm_table.h
 *
 * IDENTIFICATION
 *   src/gausskernel/storage/nvmdb/core/GaussDBKernel-nvmdb/include/nvm_table.h
 * -------------------------------------------------------------------------
 */
#ifndef NVMDB_TABLE_H
#define NVMDB_TABLE_H

#include "nvm_table_space.h"
#include "nvm_tuple.h"
#include "nvm_rowid_map.h"
#include "index/nvm_index.h"
#include <vector>

namespace NVMDB {

typedef struct TableDesc {
    ColumnDesc *col_desc = nullptr;
    uint32 col_cnt = 0;
    uint64 row_len = 0;
} TableDesc;

bool TableDescInit(TableDesc *desc, uint32 colCount);

void TableDescDestroy(TableDesc *desc);

class Table {
public:
    RowIdMap *m_rowidMap{nullptr};
    uint64 m_rowLen{0};

    Table(TableId tid, uint64 rowLen) noexcept : m_tableId(tid), m_rowLen(rowLen), m_rowidMap(nullptr)
    {}

    Table(TableId tid, TableDesc desc) noexcept
        : m_tableId(tid), m_rowLen(desc.row_len), m_desc(desc), m_rowidMap(nullptr)
    {}

    bool Ready()
    {
        return m_rowidMap != NULL;
    }

    /* 新建的表必须先申请一个 segment, 返回 segment 页号 */
    uint32 CreateSegment();

    /* 已经建好的表，重启之后需要 mount segment，传参的是 segment 页号 */
    void Mount(uint32 seghead);

    uint32 SegmentHead()
    {
        return m_seghead;
    }

    uint32 GetColIdByName(const char *name) const;

    uint32 GetColCount() const noexcept
    {
        return m_desc.col_cnt;
    }

    const ColumnDesc *GetColDesc(uint32 colIndex) const noexcept
    {
        Assert(colIndex < m_desc.col_cnt && m_desc.col_desc != nullptr);
        return &(m_desc.col_desc[colIndex]);
    }

    const ColumnDesc *GetColDesc() const noexcept
    {
        Assert(m_desc.col_desc != nullptr);
        return m_desc.col_desc;
    }

    uint64 GetRowLen() const noexcept
    {
        return m_rowLen;
    }

    uint32 GetIndexCount() const
    {
        return index.size();
    }

    NVMIndex *GetIndex(uint16 num) const
    {
        Assert(num < index.size());
        if (num < index.size()) {
            return index[num];
        } else {
            return nullptr;
        }
    }

    void AddIndex(NVMIndex *i)
    {
        index.push_back(i);
    }

    NVMIndex *DelIndex(TableId id)
    {
        NVMIndex *ret = nullptr;
        for (auto iter = index.begin(); iter != index.end(); ++iter) {
            if ((*iter)->Id() == id) {
                ret = *iter;
                index.erase(iter);
                break;
            }
        }
        return ret;
    }

    void RefCountInc()
    {
        ++refCount;
    }

    void RefCountDec()
    {
        Assert(refCount > 0);
        if (--refCount == 0) {
            Assert(isDropped);
            delete this;
        }
    }

    uint32 RefCount() const
    {
        return refCount;
    }

    bool IsDropped() const
    {
        return isDropped;
    }

    void Dropped()
    {
        isDropped = true;
    }

    TableId Id() const
    {
        return m_tableId;
    }

    bool ColIsNotNull(uint32 colIndex) const
    {
        return m_desc.col_desc[colIndex].m_isNotNull;
    }

private:
    ~Table()
    {
        TableDescDestroy(&m_desc);
    }

    TableId m_tableId{0};
    uint32 m_seghead{0};
    TableDesc m_desc;
    std::vector<NVMDB::NVMIndex *> index;
    std::atomic<uint32> refCount{0};
    std::atomic<bool> isDropped{false};
};

}  // namespace NVMDB

#endif  // NVMDB_TABLE_H