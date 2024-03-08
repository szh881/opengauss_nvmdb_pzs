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
 * nvm_table_space.h
 *
 * IDENTIFICATION
 *   src/gausskernel/storage/nvmdb/core/GaussDBKernel-nvmdb/include/table_space/nvm_table_space.h
 * -------------------------------------------------------------------------
 */
#ifndef NVMDB_TABLESPACE_H
#define NVMDB_TABLESPACE_H

#include <string>
#include <vector>
#include <mutex>

#include "nvm_types.h"
#include "nvm_block.h"
#include "nvm_utils.h"
#include "nvm_logic_file.h"

namespace NVMDB {

struct TableSegMetaData {
    uint32 m_oid;
    uint32 m_seg;
};

/* mmap 之后，core dump中读不到mmap中的数据 */

/* 第一个page为tablespace元数据页面，第二个页面为应用根页面 */
class TableSpace : public LogicFile {
    /* 存在元数据页中 */
    typedef struct FreeBlockLists {
        uint32 m_root;
    } FreeBlockLists;

    typedef struct SpaceMetaData {
        /*
         * high watermark, the first un-allocated page number,
         * it's logic page number from begin of this space.
         * free block lists store the free physical pages' num.
         */
        uint32 m_hwm;
        FreeBlockLists m_freeBlockLists[EXTSZ_TYPE_NUM];
    } SpaceMetaData;

    typedef struct TableMetaData {
        uint32 m_tableNum;
        TableSegMetaData m_segheads[0];
    } TableMetaData;

    void FblInit(ExtentSizeType extsz, uint32 spaceno);
    /* TD: ensure atomic of insert and pop */
    void FblInsert(ExtentSizeType extsz, uint32 *ptr, uint32 spaceno);
    void FblInsertList(ExtentSizeType extsz, uint32 *seghead);
    bool FblPop(ExtentSizeType extsz, uint32 *ptr, uint32 spaceno);

    /* 存在第一个 page 中, TableSpace结构体存指向它的虚拟地址的指针 */
    SpaceMetaData *m_spaceMetadata;
    TableMetaData *m_tableMetadata;

    /* 最后一个slice剩余的blocks数目 */
    uint32 CurrentSliceRestBlocks(uint32 spaceno)
    {
        uint32 hwm = m_spaceMetadata[spaceno].m_hwm % SLICE_BLOCKS;
        return (SLICE_BLOCKS - hwm);
    }

public:
    /* 1 GB for release， 10 MB for debug */
    static const size_t HEAP_SPACE_SLICE_LEN = CompileValue(1024U * 1024 * 1024, 10 * 1024U * 1024);
    static const size_t HEAP_SPACE_MAX_SLICE_NUM = 16 * 1024; /* 实际中最多16TB */

    /*  slice_addr 数组初始化的时候就设够足够大，否则当数组扩展的时候，会影响并发的读 */
    TableSpace(const char *dir, const char *name) : LogicFile(dir, name, HEAP_SPACE_SLICE_LEN, HEAP_SPACE_MAX_SLICE_NUM)
    {}

    uint32 high_water_mark(uint32 spaceno = 0)
    {
        return m_spaceMetadata[spaceno].m_hwm;
    }

    /* space num of global physical page num. */
    inline uint32 get_space_of_page(const uint32 &pageno)
    {
        return (pageno / SLICE_BLOCKS) % m_dirPathNum;
    }

    /* translate sub space logic page num to global physical page num. */
    inline uint32 get_global_page_num(const uint32 &pageno, const uint32 &spaceno)
    {
        uint32 sliceno = pageno / SLICE_BLOCKS;
        return (spaceno + sliceno * m_dirPathNum) * SLICE_BLOCKS + pageno % SLICE_BLOCKS;
    }

    /* 创建tablespace */
    void Create();

    /* mount 一个已经创建好的tablespace */
    void Mount();

    /* 数据库安全退出时调用，解除mount */
    void UnMount();

    /* drop */
    void Drop();

    /* 返回的是 root page 的虚拟地址 */
    char *RootPage();

    /* 为一个segment分配一个extent，root 为segment head，一个segment中所有page会被串起来.
     * 如果 root 为invalid值，则说明分配一个新的segment。
     */
    void AllocNewExtent(uint32 *ptr, ExtentSizeType blksz, uint32 root = NVMInvalidBlockNumber, uint32 spaceno = 0);

    /* 会把 *ptr 对应的page回收，并且*ptr 置为 NULL。一般不会掉这个函数，直接调用free_segment */
    void FreeExtent(uint32 *ptr);

    /* *ptr 对应一个segment 的root，回收整个 segment，并且置 *ptr 为NULL */
    void FreeSegment(uint32 *ptr);

    /* 将存储oid->表地址的映射写入表空间的1号page */
    void CreateTable(const TableSegMetaData &oid2Seg);

    /* 根据oid在表空间中寻找表地址 */
    uint32 SearchTable(uint32 oid);

    /* 根据表地址将drop的表数据从表空间中删除 */
    void DropTable(uint32 oid);
};

}  // namespace NVMDB

#endif  // NVMDB_TABLESPACE_H
