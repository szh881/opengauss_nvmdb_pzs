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
 * datahack.cpp
 *
 * IDENTIFICATION
 *   src/gausskernel/storage/nvmdb/core/GaussDBKernel-nvmdb/tools/datahack.cpp
 * -------------------------------------------------------------------------
 */
#include <cstring>
#include <iostream>
#include <stdio.h>

#include "nvm_dbcore.h"
#include "nvm_table_space.h"
#include "nvm_rowid_map.h"
#include "nvm_tuple.h"
#include "nvm_undo_ptr.h"
#include "nvm_undo_api.h"
#include "nvm_transaction.h"
#include "nvm_access.h"

using namespace NVMDB;

static ColumnDesc TestColDesc[] = {
    COL_DESC(COL_TYPE_INT), /* col_1 */
    COL_DESC(COL_TYPE_INT), /* col_2 */
};

static uint64 row_len = 0;
static uint32 col_cnt = 0;

static void InitColumnInfo()
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

void dump_table()
{
    uint32 seghead, row_len;
    std::cin >> seghead;
    std::cin >> row_len;
    RowId start, end;
    std::cin >> start;
    std::cin >> end;

    RowIdMap *map = GetRowIdMap(seghead, row_len);
    char *undo_record_cache = new char[4096];
    for (RowId i = start; i <= end; i++) {
        auto tuple = GenRow();
        RowIdMapEntry *entry = map->GetEntry(i);
        tuple->Deserialize(entry->nvm_addr);
        if (NVMTupleIsUsed(tuple)) {
            printf("%u version: %d, trx %llx ", i, tuple->trx_info);
            if (!TRX_INFO_IS_CSN(tuple->trx_info)) {
                TransactionInfo trx_slot;
                bool exist = GetTransactionInfo(tuple->trx_info, &trx_slot);
                if (exist) {
                    printf("(CSN: %llx, status: %d) ", trx_slot.CSN, trx_slot.status);
                } else {
                    printf("recycled ");
                }
            }
            printf("\n");
            int k = 0;
            while (!UndoRecPtrIsInValid(tuple->prev) && k < 50) {
                tuple->FetchPreVersion(undo_record_cache);
                printf("\t;version: %d, trx %llx ", k, tuple->trx_info);
                if (!TRX_INFO_IS_CSN(tuple->trx_info)) {
                    TransactionInfo trx_slot;
                    bool exist = GetTransactionInfo(tuple->trx_info, &trx_slot);
                    if (exist) {
                        printf("(CSN: %llx, status: %d) ", trx_slot.CSN, trx_slot.status);
                    } else {
                        printf("recycled ");
                    }
                }
                k++;
                printf("\n");
            }
        } else {
            printf("%u not used\n", i);
        }
        delete tuple;
    }
}

void trx_info()
{
    TransactionSlotPtr trx_info;
    scanf("%u", &trx_info);
    TransactionInfo trx_slot;
    bool exist = GetTransactionInfo(trx_info, &trx_slot);
    if (exist) {
        printf("(CSN: %llx, status: %d) ", trx_slot.CSN, trx_slot.status);
    } else {
        printf("recycled ");
    }
    printf("transaction %llx, status: %d, csn: %llx", (uint64)trx_info, trx_slot.status, trx_slot.CSN);
}

/*
 * 支持功能：
 * 1. dump 某个表的所有数据，包括undo中的版本链
 * 2. 查看 某个事务的状态。
 */
int main(int argc, char **argv)
{
    std::string dir;
    std::cin >> dir;
    BootStrap(dir.c_str());

    std::string cmd;
    while (std::cin >> cmd) {
        if (strcmp(cmd.c_str(), "dump_table") == 0) {
            dump_table();
        } else if (strcmp(cmd.c_str(), "trx_info") == 0) {
            trx_info();
        } else {
            std::cout << "unsupported command\n";
        }
    }

    ExitDBProcess();
}