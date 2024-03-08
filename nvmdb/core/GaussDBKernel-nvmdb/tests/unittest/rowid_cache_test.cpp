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
 * rowid_cache_test.cpp
 *
 * IDENTIFICATION
 *   src/gausskernel/storage/nvmdb/core/GaussDBKernel-nvmdb/tests/unittest/rowid_cache_test.cpp
 * -------------------------------------------------------------------------
 */
#include <gtest/gtest.h>  // googletest header file
#include <set>

#include "nvm_rowid_cache.h"

using namespace NVMDB;

class RowIdCacheTest : public ::testing::Test {};

TEST_F(RowIdCacheTest, BasicTest)
{
    RowIdCache cache;
    for (int i = 0; i < 100; i++) {
        cache.push_back((RowId)i);
    }

    std::set<RowId> rowid_set;

    for (int i = 0; i < 100; i++) {
        RowId rid = cache.pop();
        ASSERT_NE(rid, InvalidRowId);

        ASSERT_EQ(rowid_set.count(rid), 0);
        rowid_set.insert(rid);
    }

    RowId rid = cache.pop();
    ASSERT_EQ(rid, InvalidRowId);
}
