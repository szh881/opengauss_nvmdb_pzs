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
 * vecstore_test.cpp
 *
 * IDENTIFICATION
 *   src/gausskernel/storage/nvmdb/core/GaussDBKernel-nvmdb/tests/unittest/vecstore_test.cpp
 * -------------------------------------------------------------------------
 */
#include <gtest/gtest.h>
#include <thread>

#include "nvm_table_space.h"
#include "nvm_vecstore.h"
#include "nvm_tuple.h"
#include "test_declare.h"
#include "nvmdb_thread.h"

using namespace NVMDB;

class VecStoreTest : public ::testing::Test {
private:
    TableSpace *space;

protected:
    void SetUp() override
    {
        ParseDirectoryConfig(space_dir, true);
        InitGlobalThreadStorageMgr();
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

static const int MAX_TABLES = CompileValue(128, 4);
typedef struct FakeTables {
    uint32 segments[MAX_TABLES];
} FakeTables;
static const int TUPLE_SIZE = 127;
static const int TUPLE_NUM = 256 * 1024;

TEST_F(VecStoreTest, BasicTest)
{
    TableSpace *space = MyTableSpace();
    space->create();
    VecStore *vecstores[MAX_TABLES];

    FakeTables *tblmgr = (FakeTables *)space->root_page();
    for (int i = 0; i < MAX_TABLES; i++) {
        space->alloc_new_extent(&tblmgr->segments[i], EXTSZ_2M);
        vecstores[i] = new VecStore(space, tblmgr->segments[i], TUPLE_SIZE);
    }

    std::thread workers[MAX_TABLES];

    for (int i = 0; i < MAX_TABLES; i++) {
        workers[i] = std::thread(
            [&](int tid) {
                InitThreadLocalStorage();
                for (int j = 0; j < TUPLE_NUM; j++) {
                    RowId rid = vecstores[tid]->InsertVersion();
                    ASSERT_EQ(rid, j);
                    char *tuple = vecstores[tid]->VersionPoint(rid);
                    NVMTupleSetUsed((NVMTuple *)tuple);
                    ASSERT_EQ(NVMTupleIsUsed((NVMTuple *)tuple), true);
                    char *tuple_data = tuple + NVMTupleHeadSize;
                    tuple_data[0] = (tid + j) % 128;
                }
                DestroyThreadLocalStorage();
            },
            i);
    }

    for (int i = 0; i < MAX_TABLES; i++) {
        workers[i].join();
    }

    std::cout << "multi-thread insert finished. Start checking.\n";

    space->unmount();
    space->mount();

    for (int i = 0; i < MAX_TABLES; i++) {
        workers[i] = std::thread(
            [&](int tid) {
                int skip_step = CompileValue(1000, 100);
                for (int j = 0; j < TUPLE_NUM; j += skip_step) {
                    char *tuple = vecstores[tid]->VersionPoint(j);
                    char *tuple_data = tuple + NVMTupleHeadSize;

                    ASSERT_EQ(tuple_data[0], (tid + j) % 128);
                }
            },
            i);
    }

    for (int i = 0; i < MAX_TABLES; i++) {
        workers[i].join();
    }
}
