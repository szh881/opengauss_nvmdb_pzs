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
 * smallback.cpp
 *
 * IDENTIFICATION
 *   src/gausskernel/storage/nvmdb/core/GaussDBKernel-nvmdb/benchmarks/smallbank.cpp
 * -------------------------------------------------------------------------
 */
#include <thread>
#include <glog/logging.h>
#include <getopt.h>

#include "nvm_dbcore.h"
#include "nvm_tuple.h"
#include "nvmdb_thread.h"
#include "nvm_table.h"
#include "nvm_transaction.h"
#include "nvm_access.h"

using namespace NVMDB;

constexpr int MILL_SECOND = 1000;
constexpr int SPINLOCK_TYPE = 2;

ColumnDesc AccountColDesc[] = {COL_DESC(COL_TYPE_INT)};

TableDesc AccountDesc = {&AccountColDesc[0], sizeof(AccountColDesc) / sizeof(ColumnDesc)};

class BankBench {
    std::string dataDir;
    int accounts;
    int workers;
    Table *table;
    int runTime;
    int type;

    volatile bool onWorking;

    struct WorkerStatistics {
        union {
            struct {
                uint64 commit;
                uint64 abort;
            };
            char padding[64];
        };
    } __attribute__((aligned(64)));

    WorkerStatistics *statistics;
    volatile RowIdMapEntry *locks;

public:
    BankBench(const char *dir, int accounts, int workers, int duration, int type)
        : dataDir(dir), accounts(accounts), workers(workers), onWorking(true), runTime(duration), type(type)
    {
        statistics = new WorkerStatistics[workers];
        memset(statistics, 0, sizeof(WorkerStatistics) * workers);
        locks = NULL;
    }

    ~BankBench()
    {
        delete[] statistics;
        if (type == SPINLOCK_TYPE) {
            delete[] locks;
        }
    }

    void InitBench()
    {
        InitColumnDesc(AccountDesc.col_desc, AccountDesc.col_cnt, AccountDesc.row_len);
        if (type == 0) {
            InitDB(dataDir.c_str());
            InitThreadLocalVariables();
            table = new Table(0, AccountDesc.row_len);
            table->CreateSegment();

            RAMTuple tuple(AccountDesc.col_desc, AccountDesc.row_len);
            int balance = 0;
            tuple.SetCol(0, (char *)&balance);

            Transaction *trx = GetCurrentTrxContext();
            trx->Begin();
            for (int i = 0; i < accounts; i++) {
                RowId rowId = HeapInsert(trx, table, &tuple);
            }
            trx->Commit();
        }
    }

    void EndBench()
    {
        DestroyThreadLocalVariables();
        ExitDBProcess();
    }

    void UpdateFunc(WorkerStatistics *stats)
    {
        InitThreadLocalVariables();
        stats->commit = stats->abort = 0;

        auto rnd = new RandomGenerator();
        RAMTuple tuple(AccountDesc.col_desc, AccountDesc.row_len);
        while (onWorking) {
            RowId rowId1 = (RowId)(rnd->Next() % accounts);
            RowId rowId2 = (RowId)(rnd->Next() % accounts);
            while (rowId1 == rowId2) {
                rowId2 = (RowId)(rnd->Next() % accounts);
            }
            int transfer = rnd->Next() % 100;

            auto trx = GetCurrentTrxContext();
            trx->Begin();
            HAM_STATUS status = HeapRead(trx, table, rowId1, &tuple);
            Assert(status == HAM_SUCCESS);
            int balance = 0;
            tuple.GetCol(0, (char *)&balance);
            balance -= transfer;
            tuple.UpdateCol(0, (char *)&balance);
            status = HeapUpdate(trx, table, rowId1, &tuple);
            if (status != HAM_SUCCESS) {
                trx->Abort();
                stats->abort++;
                continue;
            }

            status = HeapRead(trx, table, rowId2, &tuple);
            Assert(status == HAM_SUCCESS);
            tuple.GetCol(0, (char *)&balance);
            balance += transfer;
            tuple.UpdateCol(0, (char *)&balance);
            status = HeapUpdate(trx, table, rowId2, &tuple);
            if (status != HAM_SUCCESS) {
                trx->Abort();
                stats->abort++;
                continue;
            }

            trx->Commit();
            stats->commit++;
        }

        DestroyThreadLocalVariables();
    }

    void ScanFunc()
    {
        InitThreadLocalVariables();
        RAMTuple tuple(AccountDesc.col_desc, AccountDesc.row_len);
        int scanTime = 0;
        auto rnd = new RandomGenerator();
        static int scanLen = 10;

        while (onWorking) {
            RowId rid = rnd->Next() % (accounts - scanLen);
            auto trx = GetCurrentTrxContext();
            trx->Begin();
            for (int i = 0; i < scanLen; i++) {
                HAM_STATUS status = HeapRead(trx, table, i + rid, &tuple);
                Assert(status == HAM_SUCCESS);
            }
            trx->Commit();
            scanTime++;
        }
        LOG(INFO) << "scan " << scanTime << " times, (" << scanTime * 1.0 / runTime / MILL_SECOND << " KQPS)";

        DestroyThreadLocalVariables();
    }

    void SimulateCsnFunc(WorkerStatistics *stats)
    {
        static volatile uint64 csn = 0;
        while (onWorking) {
            __sync_fetch_and_add(&csn, 1);
            stats->commit++;
        }
    }

    void SimulateSpinlockFunc(WorkerStatistics *stats)
    {
        auto rnd = new RandomGenerator();
        while (onWorking) {
            int id = rnd->Next() % accounts;
            RowIdMapEntry *entry = (RowIdMapEntry *)&locks[id];
            entry->lock();
            entry->unlock();
            stats->commit++;
        }
    }

    void Run()
    {
        if (type == SPINLOCK_TYPE) {
            locks = new RowIdMapEntry[accounts];
            memset((char *)locks, 0, sizeof(RowIdMapEntry) * accounts);
        }

        std::thread updateTid[workers];
        for (int i = 0; i < workers; i++) {
            if (type == 0) {
                updateTid[i] = std::thread(&BankBench::UpdateFunc, this, stats + i);
            } else if (type == 1) {
                updateTid[i] = std::thread(&BankBench::SimulateCsnFunc, this, stats + i);
            } else if (type == SPINLOCK_TYPE) {
                updateTid[i] = std::thread(&BankBench::SimulateSpinlockFunc, this, stats + i);
            }
        }
        std::thread scanTid;
        if (type == 0) {
            scanTid = std::thread(&BankBench::ScanFunc, this);
        } else {
            scanTid = std::thread([]() {});
        }

        sleep(runTime);
        onWorking = false;

        for (int i = 0; i < workers; i++) {
            updateTid[i].join();
        }
        scanTid.join();
    }

    void Report()
    {
        uint64 total_commit = 0;
        uint64 total_abort = 0;
        for (int i = 0; i < workers; i++) {
            total_abort += stats[i].abort;
            total_commit += stats[i].commit;
        }
        LOG(INFO) << "Finish test, total commit " << total_commit << " (" <<
            total_commit * 1.0 / run_time / SUBTLE_SECOND << " MQPS) total abort " <<
            total_abort << " (" << total_abort * 1.0 / run_time / SUBTLE_SECOND << " MQPS)";
    }
};

static struct option g_opts[] = {
    {"threads", required_argument, nullptr, 't'},
    {"duration", required_argument, nullptr, 'd'},
    {"accounts", required_argument, nullptr, 'a'},
    {"type", required_argument, nullptr, 'T'},
};

struct SmallBankOpts {
    int threads;
    int duration;
    int accounts;
    int type;
};

static void UsageExit()
{
    LOG(INFO) << "Command line options : smallbank <options> \n"
              << "   -h --help              : Print help message \n"
              << "   -t --threads           : Thread num\n"
              << "   -d --duration          : Duration time: (second)\n"
              << "   -t --type              : Type (0: transfer, 1: simulate csn, 2. simulate spinlock)\n"
              << "   -a --accounts          : Account Number(>0)\n";
    exit(EXIT_FAILURE);
}

SmallBankOpts ParseOpt(int argc, char **argv)
{
    SmallBankOpts opt = {.threads = 16, .duration = 10, .accounts = 1000000, .type = 0};

    while (true) {
        int idx = 0;
        int c = getopt_long(argc, argv, "ht:d:T:a:", g_opts, &idx);
        if (c == -1) {
            break;
        }

        switch (c) {
            case 'h':
                UsageExit();
                break;
            case 't':
                opt.threads = atoi(optarg);
                break;
            case 'd':
                opt.duration = atoi(optarg);
                break;
            case 'T':
                opt.type = atoi(optarg);
                break;
            case 'a':
                opt.accounts = atoi(optarg);
                break;
            default:
                LOG(ERROR) << "\nUnknown option";
                UsageExit();
                break;
        }
    }
    return opt;
}

int main(int argc, char **argv)
{
    FLAGS_logtostderr = true;
    google::InitGoogleLogging(argv[0]);

    SmallBankOpts opt = ParseOpt(argc, argv);

    BankBench bench("small_bank_data", opt.accounts, opt.threads, opt.duration, opt.type);
    bench.InitBench();
    bench.Run();
    bench.Report();
    bench.EndBench();
}