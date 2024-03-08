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
 * undo_test.cpp
 *
 * IDENTIFICATION
 *   src/gausskernel/storage/nvmdb/core/GaussDBKernel-nvmdb/tests/unittest/undo_test.cpp
 * -------------------------------------------------------------------------
 */
#include <gtest/gtest.h>  // googletest header file

#include "nvmdb_thread.h"
#include "nvm_undo_api.h"
#include "test_declare.h"

using namespace NVMDB;

class UndoTest : public ::testing::Test {
protected:
    void SetUp() override
    {
        ParseDirectoryConfig(space_dir, true);
        InitGlobalThreadStorageMgr();
        InitThreadLocalStorage();
    }
    void TearDown() override
    {
        DestroyThreadLocalStorage();
        std::experimental::filesystem::remove_all(space_dir);
    }
};

TEST_F(UndoTest, BasicTest)
{
    UndoCreate(space_dir);
    UndoExitProcess();

    for (int i = 0; i < 10; i++) {
        UndoBootStrap(space_dir);
        UndoExitProcess();
    }

    UndoBootStrap(space_dir);
    InitLocalUndoSegment();

    std::vector<std::pair<UndoRecPtr, int>> undo_ptr_arr;
    std::string PREFIX = "helloworld";
    for (int i = 0; i < 4; i++) {
        PREFIX += PREFIX;  // create a long prefix
    }

    int MAX_DATA = 20;
    int MAX_TRXS = UNDO_TRX_SLOTS + 1024;
    uint64 TEST_CSN = MIN_TRX_CSN + 1;
    char *record_cache = new char[MAX_UNDO_RECORD_CACHE_SIZE];
    for (int i = 0; i < MAX_TRXS; i++) {
        UndoTrxContext *undo_trx_ctx = AllocUndoContext();
        for (int j = 0; j < MAX_DATA; j++) {
            std::string data = PREFIX + std::to_string(j);
            auto head = (UndoRecord *)record_cache;
            head->undo_type = InvalidUndoRecordType;
            head->payload = data.length();
            memcpy(head->data, data.c_str(), data.length());
            UndoRecPtr undo_ptr = undo_trx_ctx->InsertUndoRecord(head);
            undo_ptr_arr.push_back(std::make_pair(undo_ptr, j));
        }
        undo_trx_ctx->UpdateTrxSlotStatus(TRX_COMMITTED);
        undo_trx_ctx->UpdateTrxSlotCSN(TEST_CSN);
        if (i % 10 == 0) {
            UndoSegment *undo_segment = PickSegmentForTrx();
            undo_segment->RecycleTransactionSlot(TEST_CSN + 1);
        }
        ReleaseTrxUndoContext(undo_trx_ctx);
    }

    DestroyLocalUndoSegment();
    UndoExitProcess();

    UndoBootStrap(space_dir);
    int i = 0;
    for (auto undo_ptr : undo_ptr_arr) {
        if (i % (10 * MAX_DATA) == 0) {
            continue;
        }
        UndoRecord *record = CopyUndoRecord(undo_ptr.first, record_cache);
        ASSERT_GT(record->payload, PREFIX.length());
        std::string data_prefix(record->data, PREFIX.length());
        ASSERT_STREQ(data_prefix.c_str(), PREFIX.c_str());
        std::string data(record->data + PREFIX.length(), record->payload - PREFIX.length());
        int digit = atoi(data.c_str());
        ASSERT_EQ(digit, undo_ptr.second);
        i++;
    }
    UndoExitProcess();
    delete[] record_cache;
}

TEST_F(UndoTest, UndoRecoveryTest)
{
    UndoCreate(space_dir);
    UndoExitProcess();

    UndoBootStrap(space_dir);
    InitLocalUndoSegment();

    /* no undo segment switch. */
    int MAX_TRXS = UNDO_TRX_SLOTS;
    uint64 TEST_CSN = MIN_TRX_CSN;
    char *record_cache = new char[MAX_UNDO_RECORD_CACHE_SIZE];
    for (int i = 0; i < MAX_TRXS; i++) {
        UndoTrxContext *undo_trx_ctx = AllocUndoContext();

        undo_trx_ctx->UpdateTrxSlotStatus(TRX_COMMITTED);
        undo_trx_ctx->UpdateTrxSlotCSN(TEST_CSN + i);

        UndoSegment *undo_segment = PickSegmentForTrx();
        undo_segment->RecycleTransactionSlot(TEST_CSN + UNDO_TRX_SLOTS / 2);

        ReleaseTrxUndoContext(undo_trx_ctx);
    }
    TEST_CSN += MAX_TRXS;

    DestroyLocalUndoSegment();
    UndoExitProcess();

    UndoBootStrap(space_dir);
    InitLocalUndoSegment();

    /* consume the left half slots. */
    for (int i = 0; i < MAX_TRXS / 2; i++) {
        UndoTrxContext *undo_trx_ctx = AllocUndoContext();

        undo_trx_ctx->UpdateTrxSlotStatus(TRX_COMMITTED);
        undo_trx_ctx->UpdateTrxSlotCSN(TEST_CSN + i);

        ReleaseTrxUndoContext(undo_trx_ctx);
    }

    DestroyLocalUndoSegment();
    UndoExitProcess();

    UndoBootStrap(space_dir);
    /* switched to an empty undo segment. */
    InitLocalUndoSegment();

    UndoSegment *undo_segment = PickSegmentForTrx();
    ASSERT_EQ(undo_segment->SegmentEmpty(), true);

    DestroyLocalUndoSegment();
    UndoExitProcess();
    delete[] record_cache;
}