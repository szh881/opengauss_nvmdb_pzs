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
 * tablespace_test.cpp
 *
 * IDENTIFICATION
 *   src/gausskernel/storage/nvmdb/core/GaussDBKernel-nvmdb/tests/unittest/tablespace_test.cpp
 * -------------------------------------------------------------------------
 */
#include <gtest/gtest.h>  // googletest header file
#include <set>

#include "nvm_table_space.h"
#include "test_declare.h"

using namespace NVMDB;

class TableSpaceTest : public ::testing::Test {
private:
    TableSpace *space;

protected:
    void SetUp() override
    {
        ParseDirectoryConfig(space_dir, true);
        space = new TableSpace(space_dir, "heap");
    }

    void TearDown() override
    {
        space->unmount();
        std::experimental::filesystem::remove_all(space_dir);
    }

public:
    TableSpace *MyTableSpace()
    {
        return space;
    }
};

TEST_F(TableSpaceTest, TestCreateAndMount)
{
    TableSpace *space = MyTableSpace();
    space->create();
    char *root = space->root_page();
    int testsz = 100;
    for (int i = 0; i < testsz; i++) {
        root[i] = (unsigned char)i;
    }
    space->unmount();
    space->mount();

    root = space->root_page();
    for (int i = 0; i < testsz; i++) {
#ifndef SIMULATE_MMAP
        ASSERT_EQ(root[i], i);
#endif
    }
}

static const int MAX_TABLES = 3;
typedef struct FakeTables {
    uint32 segments[MAX_TABLES];
} FakeTables;

static const int MAX_BLOCKS = 100;
typedef struct SegmentHead {
    uint32 blocks[MAX_BLOCKS];
} SegmentHead;

TEST_F(TableSpaceTest, TestAllocPage)
{
    TableSpace *space = MyTableSpace();
    space->create();

    FakeTables *tblmgr = (FakeTables *)space->root_page();

    for (int i = 0; i < MAX_TABLES; i++) {
        space->alloc_new_extent(&tblmgr->segments[i], EXTSZ_2M);
        /*
         * The first two blocks are used as metadata of tablespace and database root,
         * thus allocated block must be numbered from 2
         * If rest space of current slice can not hold one extent, it should skip the rest space
         * and allocate from the next slice.
         */
        uint32 expect_blkno = i * 256 + 2;
        if (expect_blkno + 256 > space->SLICE_BLOCKS) {
            expect_blkno += 254;
        }
        ASSERT_EQ(tblmgr->segments[i], expect_blkno);
    }

    for (int i = 0; i < MAX_TABLES; i++) {
        SegmentHead *seghead = (SegmentHead *)PageGetContent(space->relpoint_of_pageno(tblmgr->segments[i]));
        for (int j = 0; j < MAX_BLOCKS; j++) {
            space->alloc_new_extent(&seghead->blocks[j], EXTSZ_2M, tblmgr->segments[i]);
        }
    }

    uint32 old_hwm = space->high_water_mark();

    /* delete all segments */
    old_hwm = space->high_water_mark();
    for (int i = 0; i < MAX_TABLES; i++) {
        space->free_segment(&tblmgr->segments[i]);
    }

    /* ensure each allocated block is unique */
    std::set<uint32> blockset;

    /* re-alloc segments */
    for (int i = 0; i < MAX_TABLES; i++) {
        space->alloc_new_extent(&tblmgr->segments[i], EXTSZ_2M);
        ASSERT_EQ(blockset.count(tblmgr->segments[i]), 0);
        blockset.insert(tblmgr->segments[i]);

        SegmentHead *seghead = (SegmentHead *)PageGetContent(space->relpoint_of_pageno(tblmgr->segments[i]));
        for (int j = 0; j < MAX_BLOCKS; j++) {
            space->alloc_new_extent(&seghead->blocks[j], EXTSZ_2M, tblmgr->segments[i]);
            /* ensure all extents are re-used */
            ASSERT_LE(seghead->blocks[j], old_hwm);

            ASSERT_EQ(blockset.count(seghead->blocks[j]), 0);
            blockset.insert(seghead->blocks[j]);
        }
    }

    ASSERT_EQ(space->high_water_mark(), old_hwm);
}
