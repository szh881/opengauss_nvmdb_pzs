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
 * bitmap_test.cpp
 *
 * IDENTIFICATION
 *   src/gausskernel/storage/nvmdb/core/GaussDBKernel-nvmdb/tests/unittest/bitmap_test.cpp
 * -------------------------------------------------------------------------
 */
#include <gtest/gtest.h>
#include <set>
#include <thread>
#include <mutex>

#include "nvm_global_bitmap.h"

using namespace NVMDB;

class MockBarrier {
    std::atomic<int> num;

public:
    explicit MockBarrier(int n) : num(n)
    {}
    void sync()
    {
        num.fetch_sub(1);
        while (num.load() != 0) {
        }
    }
};

class GlobalBitMapTest : public ::testing::Test {};

TEST_F(GlobalBitMapTest, BasicTest)
{
    static const size_t range_num = 1024 * 64;
    GlobalBitMap gbm(range_num);

    for (int i = 0; i < 3; i++) {
        std::set<uint32> bitset;

        for (int j = 0; j < range_num; j++) {
            uint32 bit = gbm.sync_acquire();
            ASSERT_EQ(bitset.count(bit), 0);
            bitset.insert(bit);
        }
        for (uint32 j = 0; j < range_num; j++) {
            gbm.sync_release(j);
        }
    }
}

TEST_F(GlobalBitMapTest, MultiThreadTest)
{
    static const size_t range_num = CompileValue(2 * 1024 * 1024, 256 * 1024);
    static const int thread_num = CompileValue(16, 4);

    GlobalBitMap gbm(range_num);
    MockBarrier barrier(thread_num);

    std::thread workers[thread_num];
    std::set<uint32> global_bitset;
    std::mutex mtx;

    for (int i = 0; i < thread_num; i++) {
        workers[i] = std::thread(
            [&](int thread_id) {
                size_t num_per_thread = range_num / thread_num;
                std::vector<uint32> bitvec;

                /* concurrent acquire and release, no core */
                for (int i = 0; i < 10; i++) {
                    for (auto j : bitvec) {
                        gbm.sync_release(j);
                    }
                    bitvec.clear();
                    for (int j = 0; j < num_per_thread; j++) {
                        bitvec.push_back(gbm.sync_acquire());
                    }
                }

                barrier.sync();

                /* check concurrent acquire, no overlap */
                mtx.lock();
                for (auto i : bitvec) {
                    ASSERT_EQ(global_bitset.count(i), 0);
                    global_bitset.insert(i);
                }

                mtx.unlock();
            },
            i);
    }

    for (int i = 0; i < thread_num; i++) {
        workers[i].join();
    }
}
