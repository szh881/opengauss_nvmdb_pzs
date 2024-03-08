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
 * nvm_table_space.cpp
 *
 * IDENTIFICATION
 *   src/gausskernel/storage/nvmdb/core/GaussDBKernel-nvmdb/dbcore/table_space/nvm_table_space.cpp
 * -------------------------------------------------------------------------
 */
#include <iostream>
#include <cstring>

#include "nvm_table_space.h"

namespace NVMDB {

constexpr uint32 HIGH_WATER_MARK = 2;

void TableSpace::FblInit(ExtentSizeType extsz, uint32 spaceno)
{
    FreeBlockLists *fbl = &m_spaceMetadata[spaceno].m_freeBlockLists[extsz];
    fbl->m_root = NVMInvalidBlockNumber;
}

void TableSpace::FblInsert(ExtentSizeType extsz, uint32 *ptr, uint32 spaceno)
{
    FreeBlockLists *fbl = &m_spaceMetadata[spaceno].m_freeBlockLists[extsz];
    if (fbl->m_root == NVMInvalidBlockNumber) {
        fbl->m_root = *ptr;
        page_dlist_init_head(this, PageSegmentDListOffset, *ptr);
    } else {
        page_dlist_push_tail(this, PageSegmentDListOffset, fbl->m_root, *ptr);
    }

    *ptr = NVMInvalidBlockNumber;
}

void TableSpace::FblInsertList(ExtentSizeType extsz, uint32 *seghead)
{
    uint32 node = 0;
    uint32 spaceno = 0;
    /* pop until only head left. */
    while (!page_dlist_is_head(this, PageSegmentDListOffset, *seghead)) {
        node = page_dlist_pop_tail(this, PageSegmentDListOffset, *seghead);
        spaceno = get_space_of_page(node);
        FblInsert(extsz, &node, spaceno);
    }
    node = *seghead;
    spaceno = get_space_of_page(node);
    FblInsert(extsz, &node, spaceno);

    *seghead = NVMInvalidBlockNumber;
}

bool TableSpace::FblPop(ExtentSizeType extsz, uint32 *ptr, uint32 spaceno)
{
    FreeBlockLists *fbl = &m_spaceMetadata[spaceno].m_freeBlockLists[extsz];
    if (fbl->m_root == NVMInvalidBlockNumber) {
        return false;
    }

    if (page_dlist_is_head(this, PageSegmentDListOffset, fbl->m_root)) {
        /* last one */
        *ptr = fbl->m_root;
        fbl->m_root = NVMInvalidBlockNumber;
    } else {
        *ptr = page_dlist_pop_tail(this, PageSegmentDListOffset, fbl->m_root);
    }

    return true;
}

void TableSpace::Create()
{
    LogicFile::Create();
    m_spaceMetadata = reinterpret_cast<SpaceMetaData *>(m_sliceAddr[0]);
    m_tableMetadata = reinterpret_cast<TableMetaData *>(m_sliceAddr[0] + NVM_BLCKSZ);
    Assert(m_dirPathNum * sizeof(SpaceMetaData) <= NVM_BLCKSZ);
    /* first two block of space 0 kept as space meta and table meta respectively. */
    for (int i = 0; i < m_dirPathNum; i++) {
        m_spaceMetadata[i].m_hwm = (i == 0) ? HIGH_WATER_MARK : 0;
        for (int j = 0; j < EXTSZ_TYPE_NUM; j++) {
            FblInit(static_cast<ExtentSizeType>(j), i);
        }
    }
}

void TableSpace::Mount()
{
    LogicFile::Mount();
    m_spaceMetadata = reinterpret_cast<SpaceMetaData *>(m_sliceAddr[0]);
    m_tableMetadata = reinterpret_cast<TableMetaData *>(m_sliceAddr[0] + NVM_BLCKSZ);
#ifndef SIMULATE_MMAP
    Assert(m_spaceMetadata->m_hwm > 0);
#endif
    uint32 hwm = 0;
    for (uint32 i = 0; i < m_dirPathNum; i++) {
        hwm = m_spaceMetadata[i].m_hwm;
        for (uint32 j = 0; j * SLICE_BLOCKS <= hwm; j++) {
            extend(get_global_page_num(j * SLICE_BLOCKS, i));
        }
    }
}

void TableSpace::UnMount()
{
    LogicFile::UnMount();
}

void TableSpace::Drop()
{
    LogicFile::Drop();
}

char *TableSpace::RootPage()
{
    Assert(m_sliceAddr.size() > 0);
    return m_sliceAddr[0] + NVM_BLCKSZ;
}

void TableSpace::AllocNewExtent(uint32 *ptr, ExtentSizeType blksz, uint32 root, uint32 spaceno)
{
    /* pageno is physical page number. */
    uint32 pageno;
    m_spcMtx.lock();
    if (!FblPop(blksz, &pageno, spaceno)) {
        /* no free page yet, allocating a new extent. */
        uint32 restBlocks = CurrentSliceRestBlocks(spaceno);
        if (restBlocks < GetExtentBlockCount(blksz)) {
            /* Ensure the new extent should be in one slice. If the rest space can not allocate an extent, skip current
             * slice and push all rest blocks into free list of EXTENT_8k */
            for (int i = 0; i < restBlocks; i++) {
                uint32 blkno = m_spaceMetadata[spaceno].m_hwm + i;
                blkno = get_global_page_num(blkno, spaceno);
                FblInsert(EXTSZ_8K, &blkno, spaceno);
            }
            m_spaceMetadata[spaceno].m_hwm += restBlocks;
            Assert(m_spaceMetadata[spaceno].m_hwm % SLICE_BLOCKS == 0);
        }

        uint32 newHwm = m_spaceMetadata[spaceno].m_hwm + GetExtentBlockCount(blksz);
        uint32 nextSliceno = get_global_page_num(newHwm, spaceno) / SLICE_BLOCKS;

        /* ensure not exceeding file size */
        MMapFile(nextSliceno, true);
        pageno = get_global_page_num(m_spaceMetadata[spaceno].m_hwm, spaceno);
        m_spaceMetadata[spaceno].m_hwm = newHwm;
    }
    m_spcMtx.unlock();

    NVMPageHeader *pageHeader = reinterpret_cast<NVMPageHeader *>(RelpointOfPageno(pageno));
    pageHeader->m_blkno = pageno;
    pageHeader->m_blksz = blksz;

    if (NVMBlockNumberIsInvalid(root)) {
        /* 链表自己指向自己 */
        page_dlist_init_head(this, PageSegmentDListOffset, pageno);
    } else {
        page_dlist_push_tail(this, PageSegmentDListOffset, root, pageno);
    }

    char *content = PageGetContent(pageHeader);
    int ret = memset_s(content, PageContentSize(blksz), 0, PageContentSize(blksz));
    SecureRetCheck(ret);

    *ptr = pageno;
}

void TableSpace::FreeExtent(uint32 *ptr)
{
    std::lock_guard<std::mutex> guard(m_spcMtx);

    NVMPageHeader *pageHeader = reinterpret_cast<NVMPageHeader *>(RelpointOfPageno(*ptr));
    Assert(*ptr == pageHeader->m_blkno);
    uint32 spaceno = get_space_of_page(*ptr);
    FblInsert(static_cast<ExtentSizeType>(pageHeader->m_blksz), ptr, spaceno);
}

void TableSpace::FreeSegment(uint32 *ptr)
{
    std::lock_guard<std::mutex> guard(m_spcMtx);

    NVMPageHeader *pageHeader = reinterpret_cast<NVMPageHeader *>(RelpointOfPageno(*ptr));
    Assert(*ptr == pageHeader->m_blkno);
    FblInsertList(static_cast<ExtentSizeType>(pageHeader->m_blksz), ptr);
}

void TableSpace::CreateTable(const TableSegMetaData &oid2Seg)
{
    std::lock_guard<std::mutex> lockGuard(m_spcMtx);
    errno_t ret =
        memcpy_s(m_tableMetadata->m_segheads + m_tableMetadata->m_tableNum, sizeof(oid2Seg), &oid2Seg, sizeof(oid2Seg));
    SecureRetCheck(ret);
    m_tableMetadata->m_tableNum++;
}

uint32 TableSpace::SearchTable(uint32 oid)
{
    std::lock_guard<std::mutex> lockGuard(m_spcMtx);
    for (int i = 0; i < m_tableMetadata->m_tableNum; ++i) {
        TableSegMetaData *segAddr = m_tableMetadata->m_segheads + i;
        if (segAddr->m_oid == oid) {
            return segAddr->m_seg;
        }
    }
    return 0;
}

void TableSpace::DropTable(uint32 oid)
{
    int i = 0;
    TableSegMetaData *segAddr = nullptr;
    std::lock_guard<std::mutex> lockGuard(m_spcMtx);
    for (; i < m_tableMetadata->m_tableNum; ++i) {
        segAddr = m_tableMetadata->m_segheads + i;
        if (segAddr->m_oid == oid) {
            break;
        }
    }

    if (segAddr == nullptr) {
        return;
    }

    size_t count = m_tableMetadata->m_tableNum - i - 1;
    if (count != 0) {
        size_t memLen = count * sizeof(TableSegMetaData);
        errno_t ret = memmove_s(m_tableMetadata->m_segheads + i, memLen, m_tableMetadata->m_segheads + i + 1, memLen);
        SecureRetCheck(ret);
    }
    m_tableMetadata->m_tableNum--;
}

}  // namespace NVMDB
