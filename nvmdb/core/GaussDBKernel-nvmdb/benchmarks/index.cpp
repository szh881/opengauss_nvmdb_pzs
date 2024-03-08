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
 * index.cpp
 *
 * IDENTIFICATION
 *   src/gausskernel/storage/nvmdb/core/GaussDBKernel-nvmdb/benchmarks/index.cpp
 * -------------------------------------------------------------------------
 */
#include <thread>
#include <glog/logging.h>
#include <getopt.h>
#include <iostream>
#include <experimental/filesystem>

#include "nvm_dbcore.h"
#include "nvmdb_thread.h"
#include "index/nvm_index.h"
#include "index/nvm_index_access.h"
#include "nvm_transaction.h"

using namespace NVMDB;

constexpr int MAX_DURATION = 100
constexpr int MAX_TYPE_NUM = 3
constexpr int SUBTLE_SECOND = 1000000

static struct option g_opts[] = {
    {"help", no_argument, nullptr, 'h'},           {"threads", required_argument, nullptr, 't'},
    {"duration", required_argument, nullptr, 'd'}, {"warmup", required_argument, nullptr, 'a'},
    {"type", required_argument, nullptr, 'T'},
};

struct IndexBenchOpts {
    int threads;
    int duration;
    int warmup;
    int type;
};

static void UsageExit()
{
    std::cout << "Command line options : [binary] <options> \n" <<
        "   -h --help              : Print help message \n" <<
        "   -t --threads           : Thread num\n" <<
        "   -d --duration          : Duration time: (second)\n" <<
        "   -T --type              : Type (0: insert, 1: mixed, 2: lookup, 3: scan, 4: all. default 3)\n" <<
        "   -w --warmup            : Warmup Key/Value number(>0)\n";
    exit(EXIT_FAILURE);
}

IndexBenchOpts ParseOpt(int argc, char **argv)
{
    IndexBenchOpts opt = {.threads = 16, .duration = 10, .warmup = 100000, .type = 4};

    while (true) {
        int idx = 0;
        int c = getopt_long(argc, argv, "ht:d:T:w:", g_opts, &idx);
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
                if (opt.duration > MAX_DURATION) {
                    LOG(WARNING) << "duration is too long";
                }
                break;
            case 'T':
                opt.type = atoi(optarg);
                if (opt.type < 0 || opt.type > MAX_TYPE_NUM) {
                    LOG(ERROR) << "test type is illegal; please use number between 0 and 3";
                }
                break;
            case 'w':
                opt.warmup = atoi(optarg);
                break;
            default:
                LOG(ERROR) << "\nUnknown option";
                UsageExit();
                break;
        }
    }
    return opt;
}

ColumnDesc TestColDesc[] = {COL_DESC(COL_TYPE_INT), VAR_DESC(COL_TYPE_VARCHAR, 6)};

TableDesc TestDesc = {&TestColDesc[0], sizeof(TestColDesc) / sizeof(ColumnDesc)};

IndexColumnDesc TestPKColDesc[] = {{0}, {1}};

IndexDesc TestPKDesc = {&TestPKColDesc[0], sizeof(TestPKColDesc) / sizeof(IndexColumnDesc)};

class IndexBench {
    NVMIndex *idx;
    int workers;
    volatile bool onWorking;
    int runTime;
    int warmup;
    int *statistics;

public:
    IndexBench(const char *dir, IndexBenchOpts opt) : workers(opt.threads), runTime(opt.duration), warmup(opt.warmup)
    {
        InitColumnDesc(TestDesc.col_desc, TestDesc.col_cnt, TestDesc.row_len);
        InitIndexDesc(TestPKDesc.index_col_desc, &TestColDesc[0], TestPKDesc.index_col_cnt, TestPKDesc.index_len);
        if (std::experimental::filesystem::exists(dir)) {
            BootStrap(dir);
        } else {
            InitDB(dir);
        }
        InitThreadLocalVariables();
        idx = new NVMIndex(1);
        statistics = new int[opt.threads];
        memset(statistics, 0, sizeof(int) * opt.threads);
    }

    const char *prefixData = "hello";
    const int prefixLen = strlen(prefixData);

    void InsertFunc(int tid)
    {
        assert(tid < workers);
        InitThreadLocalVariables();

        int rid = warmup + tid;
        DRAMIndexTuple tuple(TestDesc.col_desc, TestPKDesc.index_col_desc, TestPKDesc.index_col_cnt,
                             TestPKDesc.index_len);
        auto trx = GetCurrentTrxContext();
        trx->Begin();
        while (onWorking) {
            tuple.SetCol(0, (char *)&rid);
            IndexInsert(trx, idx, &tuple, rid);
            rid += workers;
        }
        trx->Commit();
        statistics[tid] = (rid - warmup) / workers;

        DestroyThreadLocalVariables();
    }

    void InsertRemoveFunc(int tid)
    {
        static const int SCAN_LEN = 5;
        int k = 0;
        InitThreadLocalVariables();
        DRAMIndexTuple tuple(TestDesc.col_desc, TestPKDesc.index_col_desc, TestPKDesc.index_col_cnt,
                             TestPKDesc.index_len);

        auto trx = GetCurrentTrxContext();
        trx->Begin();
        LookupSnapshot snapshot = {0, trx->GetSnapshot()};
        while (onWorking) {
            int rid = warmup + (tid + k * workers) * SCAN_LEN;
            int temp = 0;
            for (int i = 0; i < SCAN_LEN; i++) {
                temp = rid + i;
                tuple.SetCol(0, (char *)&temp);
                IndexInsert(trx, idx, &tuple, temp);
            }
            DRAMIndexTuple start(TestDesc.col_desc, TestPKDesc.index_col_desc, TestPKDesc.index_col_cnt,
                                 TestPKDesc.index_len);
            DRAMIndexTuple end(TestDesc.col_desc, TestPKDesc.index_col_desc, TestPKDesc.index_col_cnt,
                               TestPKDesc.index_len);
            start.SetCol(0, (char *)&rid);
            temp = rid + SCAN_LEN;
            end.SetCol(0, (char *)&temp);
            auto iter = idx->GenerateIter(&start, &end, snapshot, 0, false);
            for (int i = 0; i < SCAN_LEN; i++) {
                if (!iter->Valid() || iter->Curr() != rid + i) {
                    LOG(ERROR) << "scan wrong result";
                    return;
                }
                iter->Next();
            }
            delete iter;
            IndexDelete(trx, idx, &tuple, *(int *)tuple.GetCol(0));
            k++;
        }
        trx->Commit();
        statistics[tid] = k;
        DestroyThreadLocalVariables();
    }

    void LookupFunc(int tid, int maxIdx)
    {
        int rid = tid;
        InitThreadLocalVariables();
        DRAMIndexTuple tuple(TestDesc.col_desc, TestPKDesc.index_col_desc, TestPKDesc.index_col_cnt,
                             TestPKDesc.index_len);
        auto trx = GetCurrentTrxContext();
        trx->Begin();
        LookupSnapshot snapshot = {0, trx->GetSnapshot()};
        int k = 0;
        RandomGenerator rdm;

        while (onWorking) {
            int temp = rdm.Next() % maxIdx;
            tuple.SetCol(0, (char *)&temp);
            auto iter = idx->GenerateIter(&tuple, &tuple, snapshot, 0, false);
            if (!iter->Valid() || iter->Curr() != temp) {
                LOG(ERROR) << "lookup rowid failed";
                return;
            }
            delete iter;
            k++;
        }
        statistics[tid] = k;
        DestroyThreadLocalVariables();
    }

    void ScanFunc(int tid, int maxIdx)
    {
        int k = 0;
        static const int SCAN_LEN = 5;
        InitThreadLocalVariables();
        auto trx = GetCurrentTrxContext();
        trx->Begin();

        while (onWorking) {
            DRAMIndexTuple start(TestDesc.col_desc, TestPKDesc.index_col_desc, TestPKDesc.index_col_cnt,
                                 TestPKDesc.index_len);
            DRAMIndexTuple end(TestDesc.col_desc, TestPKDesc.index_col_desc, TestPKDesc.index_col_cnt,
                               TestPKDesc.index_len);
            int temp = k % (maxIdx - SCAN_LEN);
            start.SetCol(0, (char *)&temp);
            temp += SCAN_LEN;
            end.SetCol(0, (char *)&temp);
            auto ss = GetIndexLookupSnapshot(trx);
            auto iter = idx->GenerateIter(&start, &end, ss, 0, false);
            for (int i = 0; i < SCAN_LEN; i++) {
                if (!iter->Valid()) {
                    LOG(ERROR) << "scan failed";
                    return;
                }
                if (iter->Curr() != *(int *)start.GetCol(0) + i) {
                    LOG(ERROR) << "scan wrong result";
                    return;
                }
                iter->Next();
            }
            delete iter;
            k++;
        }
        statistics[tid] = k;
    }

    enum class BENCH_TYPE {
        INSERT,
        MIX,
        LOOKUP,
        SCAN,
        BENCH_NUM,
    };

    const char *testName[BENCH_NUM] = {
        "insert test",
        "mixed test",
        "lookup",
        "scan",
    };

    void WarmUp()
    {
        auto trx = GetCurrentTrxContext();
        trx->Begin();
        DRAMIndexTuple tuple(TestDesc.col_desc, TestPKDesc.index_col_desc, TestPKDesc.index_col_cnt,
                             TestPKDesc.index_len);
        for (int i = 0; i < warmup; i++) {
            tuple.SetCol(0, (char *)&i);
            IndexInsert(trx, idx, &tuple, i);
        }
        trx->Commit();
        LOG(INFO) << "Warm up finished, insert " << warmup << " key/value pairs";
    }

    void Run(BENCH_TYPE type)
    {
        onWorking = true;
        std::thread workerTids[workers];
        for (int i = 0; i < workers; i++) {
            switch (type) {
                case INSERT:
                    workerTids[i] = std::thread(&IndexBench::InsertFunc, this, i);
                    break;
                case MIX:
                    workerTids[i] = std::thread(&IndexBench::InsertRemoveFunc, this, i);
                    break;
                case LOOKUP:
                    workerTids[i] = std::thread(&IndexBench::LookupFunc, this, i, warmup);
                    break;
                case SCAN:
                    workerTids[i] = std::thread(&IndexBench::ScanFunc, this, i, warmup);
                    break;
                default:
                    exit(0);
            }
        }
        sleep(runTime);
        onWorking = false;
        int total = 0;
        for (int i = 0; i < workers; i++) {
            workerTids[i].join();
            total += statistics[i];
        }

        LOG(INFO) << "Finish test " << testName[type] << " ops: " << total <<
            " (" << total * 1.0 / runTime / SUBTLE_SECOND << " MQPS)";
    }

    ~IndexBench()
    {
        DestroyThreadLocalVariables();
        ExitDBProcess();
    }
};

int main(int argc, char **argv)
{
    FLAGS_logtostderr = true;
    google::InitGoogleLogging(argv[0]);
    const char *dir = "testdata";

    IndexBenchOpts opt = ParseOpt(argc, argv);
    IndexBench bench(dir, opt);

    bench.WarmUp();

    if (opt.type == MAX_TYPE_NUM + 1) {
        bench.Run(IndexBench::INSERT);
        bench.Run(IndexBench::MIX);
        bench.Run(IndexBench::LOOKUP);
        bench.Run(IndexBench::SCAN);
    } else {
        bench.Run(static_cast<IndexBench::BENCH_TYPE>opt.type);
    }
}