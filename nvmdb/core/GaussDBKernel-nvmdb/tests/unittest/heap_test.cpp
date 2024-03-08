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
 * heap_test.cpp
 *
 * IDENTIFICATION
 *   src/gausskernel/storage/nvmdb/core/GaussDBKernel-nvmdb/tests/unittest/heap_test.cpp
 * -------------------------------------------------------------------------
 */
#include <glog/logging.h>
#include <gtest/gtest.h>  // googletest header file
#include <thread>

#include "nvm_dbcore.h"
#include "nvm_table.h"
#include "nvm_transaction.h"
#include "nvm_access.h"
#include "nvmdb_thread.h"
#include "test_declare.h"

using namespace NVMDB;

namespace heap_test {

ColumnDesc TestColDesc[] = {
    COL_DESC(COL_TYPE_INT), /* col_1 */
    COL_DESC(COL_TYPE_INT), /* col_2 */
};

uint64 row_len = 0;
uint32 col_cnt = 0;

void InitColumnInfo()
{
    col_cnt = sizeof(TestColDesc) / sizeof(ColumnDesc);
    InitColumnDesc(&TestColDesc[0], col_cnt, row_len);
}

inline HAM_STATUS UpdateRow(Transaction *trx, Table *table, RowId rowid, RAMTuple *tuple, int col1, int col2)
{
    ColumnUpdate updates[] = {{0, (char *)&col1}, {1, (char *)&col2}};
    tuple->UpdateCols(&updates[0], col_cnt);
    return HeapUpdate(trx, table, rowid, tuple);
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

class HeapTest : public ::testing::Test {
protected:
    void SetUp() override
    {
        InitColumnInfo();
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

TEST_F(HeapTest, HeapRecoveryTest)
{
    Table table(0, row_len);
    uint32 seghead = table.CreateSegment();
    ASSERT_EQ(NVMBlockNumberIsValid(seghead), true);

    std::vector<std::pair<RowId, RAMTuple *>> ins_set;
    for (int i = 0; i < 100; i++) {
        Transaction *trx = GetCurrentTrxContext();
        trx->Begin();
        RAMTuple *srcTuple = GenRow(true, i + 1, i + 2);
        RowId rowid = HeapInsert(trx, &table, srcTuple);

        RAMTuple *dstTuple = GenRow();
        HeapRead(trx, &table, rowid, dstTuple);
        ASSERT_EQ(dstTuple->EqualRow(srcTuple), true);
        ASSERT_EQ(dstTuple->TrxInfoIsCSN(), false);
        trx->Commit();

        trx->Begin();
        UpdateRow(trx, &table, rowid, srcTuple, i + 2, i + 3);
        trx->Commit();

        ins_set.push_back(std::make_pair(rowid, srcTuple));
        delete dstTuple;
    }

    DestroyThreadLocalVariables();
    ExitDBProcess();
    BootStrap(space_dir);
    table.Mount(seghead);
    InitThreadLocalVariables();

    for (auto item : ins_set) {
        Transaction *trx = GetCurrentTrxContext();
        trx->Begin();
        RAMTuple *dstTuple = GenRow();
        HAM_STATUS stat = HeapRead(trx, &table, item.first, dstTuple);
        ASSERT_EQ(stat, HAM_SUCCESS);
        ASSERT_EQ(dstTuple->TrxInfoIsCSN(), false);
        ASSERT_EQ(dstTuple->EqualRow(item.second), true);
        trx->Commit();

        delete dstTuple;
        delete item.second;
    }
    ins_set.clear();
}

/* 单线程的增删读写 */
TEST_F(HeapTest, BasicTest)
{
    Table table(0, row_len);
    uint32 seghead = table.CreateSegment();
    ASSERT_EQ(NVMBlockNumberIsValid(seghead), true);

    std::vector<std::pair<RowId, RAMTuple *>> ins_set;
    for (int i = 0; i < 100; i++) {
        Transaction *trx = GetCurrentTrxContext();
        trx->Begin();
        RAMTuple *srcTuple = GenRow(true, i + 1, i + 2);
        RowId rowid = HeapInsert(trx, &table, srcTuple);

        RAMTuple *dstTuple = GenRow();
        HeapRead(trx, &table, rowid, dstTuple);
        ASSERT_EQ(dstTuple->EqualRow(srcTuple), true);
        ASSERT_EQ(dstTuple->TrxInfoIsCSN(), false);
        trx->Commit();

        trx->Begin();
        UpdateRow(trx, &table, rowid, srcTuple, i + 2, i + 3);
        trx->Commit();

        ins_set.push_back(std::make_pair(rowid, srcTuple));
        delete dstTuple;
    }

    for (auto item : ins_set) {
        Transaction *trx = GetCurrentTrxContext();
        trx->Begin();
        RAMTuple *dstTuple = GenRow();
        HAM_STATUS stat = HeapRead(trx, &table, item.first, dstTuple);
        ASSERT_EQ(stat, HAM_SUCCESS);
        ASSERT_EQ(dstTuple->TrxInfoIsCSN(), false);
        ASSERT_EQ(dstTuple->EqualRow(item.second), true);
        trx->Commit();

        delete dstTuple;
        delete item.second;
    }
    ins_set.clear();
}

class ThreadSync {
    volatile int curr_step;

public:
    ThreadSync() : curr_step(0)
    {}

    void WaitOn(int step)
    {
        while (curr_step != step) {
        }
        __sync_fetch_and_add(&curr_step, 1);
    }

    void reset()
    {
        curr_step = 0;
    }
};

static RowId InsertRow(Table *table)
{
    Transaction *trx = GetCurrentTrxContext();
    trx->Begin();
    RAMTuple *srcTuple = GenRow(true, 1, 1);
    RowId rowid = HeapInsert(trx, table, srcTuple);
    trx->Commit();
    delete srcTuple;
    return rowid;
}

/* 并发的事务，其中一个读不到另外一个事务Insert的值 */
TEST_F(HeapTest, ConcurrentInsertReadTest)
{
    Table table(0, row_len);
    uint32 seghead = table.CreateSegment();
    ASSERT_EQ(NVMBlockNumberIsValid(seghead), true);

    RowId rowid = InvalidRowId;
    ThreadSync threadSync;
    std::thread tid1 = std::thread([&]() {
        InitThreadLocalVariables();
        Transaction *trx = GetCurrentTrxContext();
        trx->Begin();
        threadSync.WaitOn(0);
        threadSync.WaitOn(2); /* make sure two threads are concurrent */
        RAMTuple *srcTuple = GenRow(true, 1, 1);
        rowid = HeapInsert(trx, &table, srcTuple);
        trx->Commit();
        threadSync.WaitOn(3);
        delete srcTuple;
        DestroyThreadLocalVariables();
    });
    std::thread tid2 = std::thread([&]() {
        InitThreadLocalVariables();
        Transaction *trx = GetCurrentTrxContext();
        trx->Begin();
        threadSync.WaitOn(1);
        RAMTuple *dstTuple = GenRow();
        threadSync.WaitOn(4); /* make sure the insert transaction has committed */
        ASSERT_NE(rowid, InvalidRowId);
        HAM_STATUS status = HeapRead(trx, &table, rowid, dstTuple);
        ASSERT_EQ(status, HAM_NO_VISIBLE_VERSION);
        trx->Commit();
        delete dstTuple;
        DestroyThreadLocalVariables();
    });
    tid1.join();
    tid2.join();
}

/* 并发的事务，其中一个读不到另外一个Update的值，可以读到旧值 */
TEST_F(HeapTest, ConcurrentUpdateReadTest)
{
    Table table(0, row_len);
    uint32 seghead = table.CreateSegment();
    ASSERT_EQ(NVMBlockNumberIsValid(seghead), true);

    ThreadSync threadSync;
    RowId rowid = InsertRow(&table);
    std::thread tid1 = std::thread([&]() {
        InitThreadLocalVariables();
        Transaction *trx = GetCurrentTrxContext();
        trx->Begin();
        threadSync.WaitOn(0);
        threadSync.WaitOn(2);

        RAMTuple *srcTuple = GenRow();
        UpdateRow(trx, &table, rowid, srcTuple, 2, 2);
        trx->Commit();
        threadSync.WaitOn(3);
        delete srcTuple;
        DestroyThreadLocalVariables();
    });
    std::thread tid2 = std::thread([&]() {
        InitThreadLocalVariables();
        Transaction *trx = GetCurrentTrxContext();
        trx->Begin();
        threadSync.WaitOn(1);

        RAMTuple *dstTuple = GenRow();
        threadSync.WaitOn(4); /* make sure the update transaction has committed */
        HAM_STATUS status = HeapRead(trx, &table, rowid, dstTuple);
        ASSERT_EQ(status, HAM_SUCCESS);
        bool cmp_res = ColEqual(dstTuple, 0, 1);
        ASSERT_EQ(cmp_res, true);
        trx->Commit();

        /* 新的事务可以看到新的值 */
        trx->Begin();
        status = HeapRead(trx, &table, rowid, dstTuple);
        ASSERT_EQ(status, HAM_SUCCESS);
        cmp_res = ColEqual(dstTuple, 0, 2);
        ASSERT_EQ(cmp_res, true);
        trx->Commit();
        delete dstTuple;
        DestroyThreadLocalVariables();
    });
    tid1.join();
    tid2.join();
}

/* 并发的事务，Update 同一行，会强行让其中一个回滚 */
TEST_F(HeapTest, ConcurrentUpdateUpdateTest)
{
    Table table(0, row_len);
    Table table2(1, row_len);
    uint32 seghead = table.CreateSegment();
    ASSERT_EQ(NVMBlockNumberIsValid(seghead), true);
    table2.CreateSegment();

    ThreadSync threadSync;
    RowId rowid = InsertRow(&table);
    RowId rowid2 = InsertRow(&table2);
    std::thread tid1 = std::thread([&]() {
        InitThreadLocalVariables();
        Transaction *trx = GetCurrentTrxContext();
        trx->Begin();
        threadSync.WaitOn(0);

        RAMTuple *srcTuple = GenRow();
        HAM_STATUS status = UpdateRow(trx, &table, rowid, srcTuple, 3, 3);
        ASSERT_EQ(status, HAM_SUCCESS);
        threadSync.WaitOn(2); /* make sure the other transaction has started before I commit */
        trx->Commit();
        threadSync.WaitOn(3);
        delete srcTuple;
        DestroyThreadLocalVariables();
    });
    std::thread tid2 = std::thread([&]() {
        InitThreadLocalVariables();
        Transaction *trx = GetCurrentTrxContext();
        trx->Begin();
        threadSync.WaitOn(1);

        RAMTuple *srcTuple = GenRow();
        RAMTuple *dstTuple = GenRow();
        threadSync.WaitOn(4);
        HAM_STATUS status = UpdateRow(trx, &table2, rowid2, srcTuple, 4, 4);
        ASSERT_EQ(status, HAM_SUCCESS);
        status = HeapRead(trx, &table2, rowid2, dstTuple);
        ASSERT_EQ(status, HAM_SUCCESS);
        bool cmp_res = ColEqual(dstTuple, 0, 4);
        ASSERT_EQ(cmp_res, true);

        status = UpdateRow(trx, &table, rowid, srcTuple, 4, 4);
        ASSERT_EQ(status, HAM_UPDATE_CONFLICT);
        status = UpdateRow(trx, &table, rowid, srcTuple, 4, 4);
        ASSERT_EQ(status, HAM_TRANSACTION_WAIT_ABORT);
        trx->Abort();

        /* abort is ok */
        trx->Begin();
        status = HeapRead(trx, &table2, rowid2, dstTuple);
        ASSERT_EQ(status, HAM_SUCCESS);
        cmp_res = ColEqual(dstTuple, 0, 1);
        ASSERT_EQ(cmp_res, true);
        trx->Commit();
        delete srcTuple;
        DestroyThreadLocalVariables();
    });
    tid1.join();
    tid2.join();
}

/* 并发的事务，其中一个忽视另外一个的Delete，可以读到旧值 */
TEST_F(HeapTest, ConcurrentDeleteReadTest)
{
    Table table(0, row_len);
    uint32 seghead = table.CreateSegment();
    ASSERT_EQ(NVMBlockNumberIsValid(seghead), true);

    ThreadSync threadSync;
    RowId rowid = InsertRow(&table);
    std::thread tid1 = std::thread([&]() {
        InitThreadLocalVariables();
        Transaction *trx = GetCurrentTrxContext();
        trx->Begin();
        threadSync.WaitOn(0);
        threadSync.WaitOn(2);
        HAM_STATUS status = HeapDelete(trx, &table, rowid);
        ASSERT_EQ(status, HAM_SUCCESS);
        trx->Commit();
        threadSync.WaitOn(3);
        DestroyThreadLocalVariables();
    });

    std::thread tid2 = std::thread([&]() {
        InitThreadLocalVariables();
        Transaction *trx = GetCurrentTrxContext();
        trx->Begin();
        threadSync.WaitOn(1);
        threadSync.WaitOn(4);

        RAMTuple *dst_tuple = GenRow();
        HAM_STATUS status = HeapRead(trx, &table, rowid, dst_tuple);
        ASSERT_EQ(status, HAM_SUCCESS);
        trx->Commit();

        /* 删除事务提交之后，当前事务再读，就会发现数据已经被删了 */
        trx->Begin();
        status = HeapRead(trx, &table, rowid, dst_tuple);
        ASSERT_EQ(status, HAM_ROW_DELETED);
        trx->Abort();

        delete dst_tuple;
        DestroyThreadLocalVariables();
    });
    tid1.join();
    tid2.join();
}

/* 并发事务，一个删除，一个更新，更新的会返回 conflict 错误；如果不是并发，更新会返回 row_deleted 错误 */
TEST_F(HeapTest, ConcurrentDeleteUpdateTest)
{
    Table table(0, row_len);
    uint32 seghead = table.CreateSegment();
    ASSERT_EQ(NVMBlockNumberIsValid(seghead), true);

    ThreadSync threadSync;
    RowId rowid = InsertRow(&table);
    std::thread tid1 = std::thread([&]() {
        InitThreadLocalVariables();
        Transaction *trx = GetCurrentTrxContext();
        trx->Begin();
        threadSync.WaitOn(0);
        threadSync.WaitOn(2);
        HAM_STATUS status = HeapDelete(trx, &table, rowid);
        ASSERT_EQ(status, HAM_SUCCESS);
        trx->Commit();
        threadSync.WaitOn(3);
        DestroyThreadLocalVariables();
    });

    std::thread tid2 = std::thread([&]() {
        InitThreadLocalVariables();
        Transaction *trx = GetCurrentTrxContext();
        trx->Begin();
        threadSync.WaitOn(1);
        threadSync.WaitOn(4);

        RAMTuple *dst_tuple = GenRow();
        int col1, col2;
        dst_tuple->GetCol(0, (char *)&col1);
        dst_tuple->GetCol(0, (char *)&col2);
        HAM_STATUS status = UpdateRow(trx, &table, rowid, dst_tuple, col1, col2);
        ASSERT_EQ(status, HAM_UPDATE_CONFLICT);
        trx->Abort();

        trx->Begin();
        status = UpdateRow(trx, &table, rowid, dst_tuple, col1, col2);
        ASSERT_EQ(status, HAM_ROW_DELETED);
        trx->Abort();

        delete dst_tuple;
        DestroyThreadLocalVariables();
    });
    tid1.join();
    tid2.join();
}

static RowId PressureInsert(Table *data_tbl, Table *cnt_tbl, RowId cnt_rowid)
{
    Transaction *trx = GetCurrentTrxContext();
    trx->Begin();

    RAMTuple *src_tuple = GenRow(true, 0, 0);
    RowId rowid = HeapInsert(trx, data_tbl, src_tuple);
    RAMTuple *dst_tuple = GenRow();
    HAM_STATUS status = HeapRead(trx, cnt_tbl, cnt_rowid, dst_tuple);
    int temp;
    dst_tuple->GetCol(0, (char *)&temp);
    if (rowid > temp) {
        temp = rowid;
    }
    status = UpdateRow(trx, cnt_tbl, cnt_rowid, dst_tuple, temp, 0);
    delete src_tuple;
    delete dst_tuple;

    if (status == HAM_SUCCESS) {
        trx->Commit();
        //        DLOG(INFO) << "Transaction " << std::hex << trx->GetTrxSlotLocation() << " csn " << trx->GetCSN() <<
        //        std::dec
        //                   << " Insert one account " << rowid;
        return rowid;
    } else {
        trx->Abort();
        return InvalidRowId;
    }
}

static void PressureUpdate(Table *data_tbl, RowId rowid1, RowId rowid2, bool *commit)
{
    int transfer = 1 + random() % 100;
    Transaction *trx = GetCurrentTrxContext();
    trx->Begin();

    int val1, val2;
    RAMTuple *dst_tuple = GenRow();

    HAM_STATUS status = HeapRead(trx, data_tbl, rowid1, dst_tuple);
    ASSERT_EQ(status, HAM_SUCCESS);
    int temp = 0;
    dst_tuple->GetCol(0, (char *)&temp);
    temp -= transfer;
    val1 = temp;
    status = UpdateRow(trx, data_tbl, rowid1, dst_tuple, temp, 0);
    if (status != HAM_SUCCESS) {
        trx->Abort();
        //        DLOG(INFO) << "Transaction " << std::hex << trx->GetTrxSlotLocation() << " csn " << trx->GetCSN()
        //                   << " snapshot " << trx->GetSnapshot() << std::dec << " Try to update " << rowid1 << " " <<
        //                   val1
        //                   << " rollback";
        delete dst_tuple;
        *commit = false;
        return;
    }

    status = HeapRead(trx, data_tbl, rowid2, dst_tuple);
    Assert(status == HAM_SUCCESS);
    temp = 0;
    dst_tuple->GetCol(0, (char *)&temp);
    temp += transfer;
    val2 = temp;
    status = UpdateRow(trx, data_tbl, rowid2, dst_tuple, temp, 0);
    if (status != HAM_SUCCESS) {
        trx->Abort();
        //        DLOG(INFO) << "Transaction " << std::hex << trx->GetTrxSlotLocation() << " csn " << trx->GetCSN()
        //                   << " snapshot " << trx->GetSnapshot() << std::dec << " Try to update " << rowid1 << " " <<
        //                   val1 << " "
        //                   << rowid2 << " " << val2 << " rollback";
        delete dst_tuple;
        *commit = false;
        return;
    }

    trx->Commit();

    //    DLOG(INFO) << "Transaction " << std::hex << trx->GetTrxSlotLocation() << " csn " << trx->GetCSN() << "
    //    snapshot "
    //               << trx->GetSnapshot() << std::dec << " Update two account " << rowid1 << "(" << val1 << ") -> " <<
    //               rowid2
    //               << "(" << val2 << ")";
    delete dst_tuple;
    *commit = true;
}

static void PressureScanAll(Table *table, Table *table_cnt, RowId cnt_rowid, int success_update)
{
    Transaction *trx = GetCurrentTrxContext();
    trx->Begin();
    RAMTuple *dst_tuple = GenRow();
    HAM_STATUS status = HeapRead(trx, table_cnt, cnt_rowid, dst_tuple);
    // std::vector<RAMTuple *> tuples;

    ASSERT_EQ(status, HAM_SUCCESS);

    int max_rowid = 0;
    dst_tuple->GetCol(0, (char *)&max_rowid);
    int sum = 0;
    int valid_row = 0;
    int temp = 0;
    for (int i = 0; i <= max_rowid; i++) {
        status = HeapRead(trx, table, (RowId)i, dst_tuple);
        if (status == HAM_SUCCESS) {
            dst_tuple->GetCol(0, (char *)&temp);
            sum += temp;
            valid_row++;
            // dst_tuple->col2 = i;
            // tuples.push_back(new TestTuple(*dst_tuple));
        }
    }
    ASSERT_EQ(sum, 0);
    trx->Commit();
    // printf("max_rowid %d, valid_row: %d, sum: %d (successful update: %d)\n", max_rowid, valid_row, sum,
    // success_update);
}

static std::pair<RowId, RowId> select_transfer_accounts(std::vector<RowId> *valid_rowids)
{
    int rowid_size = valid_rowids->size();
    Assert(rowid_size >= 2);

    int k1 = random() % rowid_size;
    int k2 = random() % rowid_size;
    while (k1 == k2) {
        k2 = random() % rowid_size;
    }
    RowId rowid1 = (*valid_rowids)[k1];
    RowId rowid2 = (*valid_rowids)[k2];
    Assert(rowid1 != rowid2);
    return std::make_pair(rowid1, rowid2);
}

/* 大量的并发增删读写 */
TEST_F(HeapTest, ConcurrentPressureTest)
{
    Table table(0, row_len);
    Table table_cnt(1, row_len);
    uint32 seghead = table.CreateSegment();
    uint32 seghead2 = table_cnt.CreateSegment();
    ASSERT_EQ(NVMBlockNumberIsValid(seghead), true);
    ASSERT_EQ(NVMBlockNumberIsValid(seghead2), true);

    Transaction *trx = GetCurrentTrxContext();
    trx->Begin();
    RAMTuple *cnt = GenRow(true, 0, 0);
    RowId cnt_rowid = HeapInsert(trx, &table_cnt, cnt);
    ASSERT_NE(cnt_rowid, InvalidRowId);
    trx->Commit();
    delete cnt;

    ThreadSync threadSync;
    std::vector<RowId> valid_rowids;
    std::mutex mtx;
    volatile int success_update = 0;

    static int thread_num = 16;
    std::thread tid[thread_num];
    for (int i = 0; i < thread_num; i++) {
        tid[i] = std::thread([&]() {
            InitThreadLocalVariables();
            for (int j = 0; j < 500; j++) {
                RowId rowid = PressureInsert(&table, &table_cnt, cnt_rowid);
                mtx.lock();
                if (RowIdIsValid(rowid)) {
                    valid_rowids.push_back(rowid);
                }
                mtx.unlock();

                for (int k = 0; k < 5; k++) {
                    bool ok = false;
                    mtx.lock();
                    if (valid_rowids.size() < 2) {
                        mtx.unlock();
                        continue;
                        ;
                    }
                    auto rowids = select_transfer_accounts(&valid_rowids);
                    mtx.unlock();
                    PressureUpdate(&table, rowids.first, rowids.second, &ok);
                    if (ok) {
                        __sync_fetch_and_add(&success_update, 1);
                    }
                }
            }
            DestroyThreadLocalVariables();
        });
    }

    volatile bool on_working = true;
    std::thread bg = std::thread([&]() {
        InitThreadLocalVariables();
        do {
            PressureScanAll(&table, &table_cnt, cnt_rowid, success_update);
            usleep(100 * 1000);
        } while (on_working);
        DestroyThreadLocalVariables();
    });

    for (int i = 0; i < thread_num; i++) {
        tid[i].join();
    }
    on_working = false;
    bg.join();
    PressureScanAll(&table, &table_cnt, cnt_rowid, success_update);
}

}  // namespace heap_test