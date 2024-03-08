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
 * index_test.cpp
 *
 * IDENTIFICATION
 *   src/gausskernel/storage/nvmdb/core/GaussDBKernel-nvmdb/tests/unittest/index_test.cpp
 * -------------------------------------------------------------------------
 */
#include <gtest/gtest.h>
#include <thread>

#include "nvmdb_thread.h"
#include "index/nvm_index.h"
#include "index/nvm_index_access.h"
#include "nvm_dbcore.h"
#include "nvm_transaction.h"
#include "test_declare.h"

using namespace NVMDB;

namespace index_test {

ColumnDesc TestColDesc[] = {
    COL_DESC(COL_TYPE_INT),        /* col_1 */
    COL_DESC(COL_TYPE_INT),        /* col_2 */
    VAR_DESC(COL_TYPE_VARCHAR, 32) /* name */
};

uint64 row_len = 0;
uint32 col_cnt = 0;

void InitColumnInfo()
{
    col_cnt = sizeof(TestColDesc) / sizeof(ColumnDesc);
    InitColumnDesc(&TestColDesc[0], col_cnt, row_len);
}

inline RAMTuple *GenRow(bool value_set = false, int col1 = 0, int col2 = 0)
{
    RAMTuple *tuple = new RAMTuple(&TestColDesc[0], row_len);
    if (value_set) {
        tuple->SetCol(0, (char *)&col1);
        tuple->SetCol(1, (char *)&col2);
    }
    return tuple;
}

inline bool ColEqual(RAMTuple *tuple, int col_id, int col_val)
{
    return tuple->ColEqual(col_id, (char *)&col_val);
}

IndexColumnDesc TestIndexDesc[] = {{0}, {1}, {2}};

IndexColumnDesc TestIndexDesc2[] = {{0}};

uint64 index_len = 0;
uint32 index_col_cnt = 0;

uint64 index_len2 = 0;
uint32 index_col_cnt2 = 0;

void InitIndexInfo()
{
    index_col_cnt = sizeof(TestIndexDesc) / sizeof(IndexColumnDesc);
    index_col_cnt2 = sizeof(TestIndexDesc2) / sizeof(IndexColumnDesc);
    InitIndexDesc(&TestIndexDesc[0], &TestColDesc[0], index_col_cnt, index_len);
    InitIndexDesc(&TestIndexDesc2[0], &TestColDesc[0], index_col_cnt2, index_len2);
}

inline DRAMIndexTuple *GenIndexTuple(int col1, int col2, const char *const name)
{
    DRAMIndexTuple *tuple = new DRAMIndexTuple(&TestColDesc[0], &TestIndexDesc[0], index_col_cnt, index_len);
    tuple->SetCol(0, (char *)&col1);
    tuple->SetCol(1, (char *)&col2);
    tuple->SetCol(2, name, true);
    return tuple;
}

inline DRAMIndexTuple *GenIndexTuple2()
{
    DRAMIndexTuple *tuple = new DRAMIndexTuple(&TestColDesc[0], &TestIndexDesc2[0], index_col_cnt2, index_len2);
    return tuple;
}

class IndexTest : public ::testing::Test {
protected:
    void SetUp() override
    {
        InitColumnInfo();
        InitIndexInfo();
        InitDB(space_dir);
        ExitDBProcess();
        BootStrap(space_dir);
        InitThreadLocalVariables();
    }
    void TearDown() override
    {
        DestroyThreadLocalVariables();
        ExitDBProcess();
    }
};

static void BasicTestUnit(int si, int ei, NVMIndex &idx)
{
    std::string name = "NVMDB";
    LookupSnapshot snapshot = {0, 0};
    int TEST_NUM = ei - si;
    /* 1. insert */
    auto trx = GetCurrentTrxContext();
    trx->Begin();
    trx->PrepareUndo();
    for (int i = si; i < ei; i++) {
        DRAMIndexTuple *tuple = GenIndexTuple(1, i, name.c_str());
        snapshot.snapshot = trx->GetSnapshot();
        idx.Insert(tuple, i);
        /* Search the result */
        auto iter = idx.GenerateIter(tuple, tuple, snapshot, 10, false);
        ASSERT_EQ(iter->Valid(), true);
        ASSERT_EQ(iter->Curr(), i);
        delete iter;
        delete tuple;
    }
    trx->Commit();

    /* 2. scan */
    {
        DRAMIndexTuple *start = GenIndexTuple(1, si, name.c_str());
        DRAMIndexTuple *end = GenIndexTuple(1, ei - 1, name.c_str());
        auto iter = idx.GenerateIter(start, end, snapshot, 0, false);
        int fetch_num = 0;
        while (iter->Valid()) {
            ASSERT_EQ(iter->Curr(), si + fetch_num);
            iter->Next();
            fetch_num++;
        }
        ASSERT_EQ(fetch_num, TEST_NUM);
        delete iter;
        delete start;
        delete end;
    }

    /* 3. delete */
    trx->Begin();
    trx->PrepareUndo();
    for (int i = si; i < ei; i++) {
        DRAMIndexTuple *tuple = GenIndexTuple(1, i, name.c_str());
        snapshot.snapshot = trx->GetSnapshot();
        idx.Delete(tuple, i, trx->GetTrxSlotLocation());
        delete tuple;
    }
    trx->Commit();

    {
        auto trx = GetCurrentTrxContext();
        trx->Begin();
        snapshot.snapshot = trx->GetSnapshot();
        DRAMIndexTuple *start = GenIndexTuple(1, si, name.c_str());
        DRAMIndexTuple *end = GenIndexTuple(1, ei - 1, name.c_str());
        auto iter = idx.GenerateIter(start, end, snapshot, TEST_NUM, false);
        ASSERT_EQ(iter->Valid(), false);
        delete iter;
        delete start;
        delete end;
        trx->Commit();
    }
}

TEST_F(IndexTest, BasicTest)
{
    NVMIndex idx(1);
    BasicTestUnit(0, 100, idx);
}

TEST_F(IndexTest, ConcurrentTest)
{
    NVMIndex idx(1);

    static const int WORKER_NUM = 16;
    static const int SCAN_LEN = 100;
    volatile int on_working = true;

    std::thread workers[WORKER_NUM];
    for (int i = 0; i < WORKER_NUM; i++) {
        workers[i] = std::thread(
            [&](int tid) {
                InitThreadLocalVariables();
                int k = tid * SCAN_LEN;
                while (on_working) {
                    BasicTestUnit(k, k + SCAN_LEN, idx);
                    k += WORKER_NUM * SCAN_LEN;
                }
                DestroyThreadLocalVariables();
            },
            i);
    }
    sleep(1);
    on_working = false;
    for (int i = 0; i < WORKER_NUM; i++) {
        workers[i].join();
    }
}

TEST_F(IndexTest, TransactionTest)
{
    Table table(0, row_len);
    NVMIndex idx(1);
    uint32 seghead = table.CreateSegment();
    ASSERT_EQ(NVMBlockNumberIsValid(seghead), true);

    Transaction *trx = GetCurrentTrxContext();
    trx->Begin();
    int si = 0, ei = 10;
    int test_num = ei - si;
    for (int i = si; i < ei; i++) {
        RAMTuple *tuple = GenRow(true, i, i + 1);
        RowId rowid = HeapInsert(trx, &table, tuple);
        DRAMIndexTuple *idx_tuple = GenIndexTuple2();
        tuple->GetCol(0, idx_tuple->GetCol(0));
        IndexInsert(trx, &idx, idx_tuple, rowid);
        delete tuple;
        delete idx_tuple;
    }
    trx->Commit();

    DRAMIndexTuple *idx_begin = GenIndexTuple2();
    DRAMIndexTuple *idx_end = GenIndexTuple2();
    idx_begin->SetCol(0, (char *)&si);
    int temp = ei - 1;
    idx_end->SetCol(0, (char *)&temp);
    int res_size;
    RowId *row_ids = new RowId[test_num];
    RAMTuple **tuples = new RAMTuple *[test_num];
    for (int i = 0; i < test_num; i++) {
        tuples[i] = GenRow();
    }

    trx->Begin();
    RangeSearch(trx, &idx, &table, idx_begin, idx_end, test_num, &res_size, row_ids, tuples);

    ASSERT_EQ(res_size, test_num);
    for (int i = 0; i < res_size; i++) {
        ASSERT_EQ(ColEqual(tuples[i], 0, i + si), true);
        ASSERT_EQ(ColEqual(tuples[i], 1, i + si + 1), true);
    }

    trx->Commit();

    trx->Begin();
    RowId row_id = UniqueSearch(trx, &idx, &table, idx_begin, tuples[0]);
    ASSERT_NE(row_id, InvalidRowId);
    HAM_STATUS status = HeapDelete(trx, &table, row_id);
    ASSERT_EQ(status, HAM_SUCCESS);
    IndexDelete(trx, &idx, idx_begin, row_id);
    trx->Commit();

    trx->Begin();
    row_id = UniqueSearch(trx, &idx, &table, idx_begin, tuples[0]);
    ASSERT_EQ(row_id, InvalidRowId);
    trx->Commit();

    trx->Begin();
    row_id = RangeSearchMin(trx, &idx, &table, idx_begin, idx_end, tuples[0]);
    ASSERT_NE(row_id, InvalidRowId);
    ASSERT_EQ(ColEqual(tuples[0], 0, si + 1), true);  // 第一个已经被删除了
    ASSERT_EQ(ColEqual(tuples[0], 1, si + 2), true);

    row_id = RangeSearchMax(trx, &idx, &table, idx_begin, idx_end, tuples[0]);
    ASSERT_NE(row_id, InvalidRowId);
    ASSERT_EQ(ColEqual(tuples[0], 0, ei - 1), true);  // 最后一个
    ASSERT_EQ(ColEqual(tuples[0], 1, ei), true);
    trx->Commit();

    delete[] row_ids;
    for (int i = 0; i < test_num; i++) {
        delete tuples[i];
    }
    delete[] tuples;
    delete idx_begin;
    delete idx_end;
}

}  // namespace index_test
