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
 * nvm_index_undo.cpp
 *
 * IDENTIFICATION
 *   src/gausskernel/storage/nvmdb/core/GaussDBKernel-nvmdb/dbcore/index/nvm_index_undo.cpp
 * -------------------------------------------------------------------------
 */
#include "index/nvm_index_access.h"
#include "undo/nvm_undo_record.h"

namespace NVMDB {

/*
 * Insert 比较特殊，undo 的时候不能直接删除，因为可能存在这一的场景，
 *      trx 1  删除 IndexTuple,  CSN 为 c1
 *      trx 2  插入 IndexTuple, 把对应value 改成了 InvalidCSN
 *      trx 2  回滚，如果直接删除该 KV， 则在 trx 1 之前的事务是看不到该索引的，所以不能直接删除，而是
 *              回填一个大于等于的CSN，这样确保原来能看到这个IndexTuple的事务，依然能看到。
 *              这里有一个 trick， trx 1提交之后，trx 2才能插入，否则会有并发更新的问题，
 *              所以 trx2 的 snapshot 必然 大于 trx1 的CSN，所以直接用 trx2 的snapshot 回填即可。
 * undo 的格式
 * 因为UndoRecord 的head对索引undo来说没用，所以复用了下存储空间。seghead 和 rowid 两个 uint32 拼成了一个 uint64 的CSN
 */
void PrepareIndexInsertUndo(Transaction *trx, Key_t &key, uint64 csn)
{
    auto *undo = reinterpret_cast<UndoRecord *>(trx->undoRecordCache);
    undo->m_undoType = IndexInsertUndo;
    undo->m_rowLen = 0;
    undo->m_seghead = csn >> BIS_PER_U32;
    undo->m_rowId = csn & 0xFFFFFFFF;
    undo->m_payload = sizeof(key);
    undo->m_pre = 0;
#ifndef NDEBUG
    undo->m_trxSlot = trx->GetTrxSlotLocation();
#endif
    int ret = memcpy_s(undo->data, MAX_UNDO_RECORD_CACHE_SIZE, &key, sizeof(key));
    SecureRetCheck(ret);
    trx->InsertUndoRecord(undo);
}

/*
 * 删除的时候可以保证，自己可以看见，且没有并发的修改，即在删除之前肯定是可见的。所以回滚直接设置value 为InvalidCSN
 * 即可。
 */
void PrepareIndexDeleteUndo(Transaction *trx, Key_t &key)
{
    auto *undo = reinterpret_cast<UndoRecord *>(trx->undoRecordCache);
    undo->m_undoType = IndexDeleteUndo;
    undo->m_rowLen = 0;
    undo->m_seghead = NVMInvalidBlockNumber;
    undo->m_rowId = InvalidRowId;
    undo->m_payload = sizeof(key);
    undo->m_pre = 0;
    int ret = memcpy_s(undo->data, MAX_UNDO_RECORD_CACHE_SIZE, &key, sizeof(key));
    SecureRetCheck(ret);
    trx->InsertUndoRecord(undo);
}

void UndoIndexInsert(UndoRecord *undo)
{
    uint64 csn = undo->m_seghead;
    csn = (csn << BIS_PER_U32) | undo->m_rowId;
    Key_t key;
    Assert(undo->m_payload == sizeof(Key_t));
    int ret = memcpy_s(&key, sizeof(key), undo->data, undo->m_payload);
    SecureRetCheck(ret);
    auto pt = GetGlobalPACTree();
    pt->Insert(key, csn);
}

void UndoIndexDelete(UndoRecord *undo)
{
    Key_t key;
    Assert(undo->m_payload == sizeof(Key_t));
    int ret = memcpy_s(&key, sizeof(key), undo->data, undo->m_payload);
    SecureRetCheck(ret);
    auto pt = GetGlobalPACTree();
    pt->Insert(key, INVALID_CSN);
}

}  // namespace NVMDB