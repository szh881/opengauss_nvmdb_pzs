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
 * nvm_undo_page.h
 *
 * IDENTIFICATION
 *   src/gausskernel/storage/nvmdb/core/GaussDBKernel-nvmdb/include/undo/nvm_undo_page.h
 * -------------------------------------------------------------------------
 */
#ifndef NVMDB_UNDO_PAGE_H
#define NVMDB_UNDO_PAGE_H

#include "nvm_types.h"
#include "nvm_page_dlist.h"
#include "nvm_undo_ptr.h"
#include "heap/nvm_tuple.h"
#include "nvm_block.h"

namespace NVMDB {

enum TransactionSlotStatus {
    TRX_EMPTY = 0,   /* slot 还没有被分配 */
    TRX_IN_PROGRESS = 1, /* 事务正在进行中 */
    TRX_COMMITTED, /* 事务已提交，等过一段时间undo就可以失效了 */
    TRX_ABORTED,   /* 事务已经回滚，但是没有完成undo */
    TRX_ROLLBACKED   /* undo完成，transaction slot可复用 */
};

/* 持久化的事务信息 */
typedef struct TransactionSlot {
    volatile uint64 CSN;
    UndoRecPtr start;
    UndoRecPtr end;
    volatile uint32 status;
} TransactionSlot;

typedef struct TransactionInfo {
    TransactionSlotStatus status;
    uint64 CSN;
} TransactionInfo;

static inline TransactionInfo GetTransactionInfoSafe(TransactionSlot *slot)
{
    TransactionInfo res;
    res.CSN = slot->CSN;
    res.status = (TransactionSlotStatus) slot->status;

    while (res.status == TRX_COMMITTED && !TrxInfoIsCsn(res.CSN)) {
        /* 事务正在提交，但是CSN还没来得及设 */
        res.CSN = slot->CSN;
    }
    return res;
}

typedef struct UndoPageHeader {
    PageDListNode trx_page_list;
    uint32 free_space_begin;
} UndoPageHeader;

static const int UndoTrxPageDListOffset = PageHeaderSize;
static const size_t UndoPageMetaSize =  PageHeaderSize + sizeof(UndoPageHeader);
static const size_t UndoPageContentSize = GetExtentBlockCount(EXTSZ_8K) - UndoPageMetaSize;

void InitUndoPage(TableSpace* space, uint32 pageno);

bool PageAbleContain(TableSpace *space, uint32 pageno, int data_len);

void UndoPageRollBack(TableSpace *space, uint32 pageno);

}

#endif // NVMDB_UNDO_PAGE_H