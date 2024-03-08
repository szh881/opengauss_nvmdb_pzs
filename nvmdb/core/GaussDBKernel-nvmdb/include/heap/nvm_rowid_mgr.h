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
 * nvm_rowid_mgr.h
 *
 * IDENTIFICATION
 *   src/gausskernel/storage/nvmdb/core/GaussDBKernel-nvmdb/include/heap/nvm_rowid_mgr.h
 * -------------------------------------------------------------------------
 */
#ifndef NVMDB_ROWID_MGR_H
#define NVMDB_ROWID_MGR_H

#include <mutex>

#include "nvm_types.h"
#include "nvm_table_space.h"

namespace NVMDB {

static const ExtentSizeType HEAP_EXTENT_SIZE = EXTSZ_2M;

class RowIDMgr {
    uint32 seghead;
    uint32 tuple_len;
    uint32 tuples_perpage;
    TableSpace *tblspc;
    std::mutex mtx;

    std::pair<uint32, uint32> RowIdToLeafPageLocation(RowId rid)
    {
        int pageid = rid / tuples_perpage;
        int page_offset = rid % tuples_perpage;
        return std::make_pair(pageid, page_offset);
    }

    uint32 *GetRootPageMap()
    {
        char *rootpage = tblspc->RelpointOfPageno(seghead);
        /* NVMPageHeader + MaxPageNum + Page Maps */
        uint32 *map = (uint32 *)PageGetContent(rootpage) + 1;
        return map;
    }

    inline void SetMaxPageNum(uint32 &page_num)
    {
        char *rootpage = tblspc->RelpointOfPageno(seghead);
        uint32 *max_page_num = (uint32 *)PageGetContent(rootpage);
        if (*max_page_num < page_num) {
            *max_page_num = page_num;
        }
    }

    inline uint32 GetMaxPageNum()
    {
        char *rootpage = tblspc->RelpointOfPageno(seghead);
        return *(uint32 *)PageGetContent(rootpage);
    }

    void try_alloc_new_page(uint32 leaf_page_idx)
    {
        uint32 *map = GetRootPageMap();

        std::lock_guard<std::mutex> lock_guard(mtx);
        if (NVMBlockNumberIsValid(map[leaf_page_idx])) {
            return;
        }
        /* leaf page idx is logic page number. allocate physical page from space no. */
        uint32 spaceno = leaf_page_idx % g_dirPathNum;
        tblspc->AllocNewExtent(&map[leaf_page_idx], HEAP_EXTENT_SIZE, seghead, spaceno);
    }

public:
    RowIDMgr(TableSpace *_space, uint32 _seghead, uint32 _tuple_len)
        : tblspc(_space), seghead(_seghead), tuple_len(_tuple_len)
    {
        tuples_perpage = PageContentSize(HEAP_EXTENT_SIZE) / tuple_len;
    }

    char *version_pointer(RowId rowid, bool append = true)
    {
        auto leaf_page_loc = RowIdToLeafPageLocation(rowid);
        uint32 *map = GetRootPageMap();

        /* 1. check leaf page existing. If not, try to allocate a new page */
        if (NVMBlockNumberIsInvalid(map[leaf_page_loc.first])) {
            if (append) {
                SetMaxPageNum(leaf_page_loc.first);
                try_alloc_new_page(leaf_page_loc.first);
            } else {
                return NULL;
            }
        }

        uint32 pagenum = map[leaf_page_loc.first];
        Assert(NVMBlockNumberIsValid(pagenum));
        char *leafpage = tblspc->RelpointOfPageno(pagenum);
        char *leafdata = (char *)PageGetContent(leafpage);
        char *tuple = leafdata + leaf_page_loc.second * tuple_len;

        return tuple;
    }

    inline RowId GetUpperRowId()
    {
        return (GetMaxPageNum() + 1) * tuples_perpage;
    }
};

}  // namespace NVMDB

#endif  // NVMDB_ROWID_MGR_H