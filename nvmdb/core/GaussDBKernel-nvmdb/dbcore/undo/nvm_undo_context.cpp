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
 * nvm_undo_context.cpp
 *
 * IDENTIFICATION
 *   src/gausskernel/storage/nvmdb/core/GaussDBKernel-nvmdb/dbcore/undo/nvm_undo_context.cpp
 * -------------------------------------------------------------------------
 */
#include <cstring>

#include "nvm_undo_context.h"

namespace NVMDB {

UndoTrxContext *AllocUndoContext()
{
    SwitchUndoSegmentIfFull();
    UndoSegment *undo_segment = PickSegmentForTrx();
    uint64 trxslot_id = undo_segment->GetNextTrxSlot();
    UndoTrxContext *result = new UndoTrxContext(undo_segment, trxslot_id);
    /*
     * As the undo recycle runs background,
     * Only after modified the allocated trxslot, it's safe to advance.
     */
    undo_segment->AdvanceTrxSlot();
    return result;
}

UndoRecPtr UndoTrxContext::InsertUndoRecord(UndoRecord *record)
{
    record->m_pre = trxslot->end;
    UndoRecPtr undo = undo_segment->InsertUndoRecord(record);
    if (UndoRecPtrIsInValid(trxslot->start)) {
        trxslot->start = undo;
    }
    Assert(trxslot->end < undo);
    trxslot->end = undo;
    Assert(trxslot->end >= trxslot->start);

    return undo;
}

void ReleaseTrxUndoContext(UndoTrxContext *undo_trx_ctx)
{
    delete undo_trx_ctx;
}

}
