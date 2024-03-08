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
 * nvm_undo_context.h
 *
 * IDENTIFICATION
 *   src/gausskernel/storage/nvmdb/core/GaussDBKernel-nvmdb/include/undo/nvm_undo_context.h
 * -------------------------------------------------------------------------
 */
#ifndef NVMDB_UNDO_CONTEXT_H
#define NVMDB_UNDO_CONTEXT_H

#include "nvm_undo_record.h"
#include "nvm_undo_segment.h"

namespace NVMDB {

/* undo context，跟一个事务绑定 */
class UndoTrxContext {
    uint64 trxslot_id;
    TransactionSlot *trxslot;
    UndoSegment *undo_segment;
public:
    UndoTrxContext(UndoSegment *_undo_segment, uint32 _trxslot_id)
        : trxslot_id(_trxslot_id), undo_segment(_undo_segment)
    {
        trxslot = undo_segment->GetTransactionSlot(trxslot_id);
        trxslot->status = TRX_IN_PROGRESS;
    }

    void UpdateTrxSlotCSN(uint64 csn)
    {
        trxslot->CSN = csn;
    }
    void UpdateTrxSlotStatus(TransactionSlotStatus status)
    {
        trxslot->status = status;
    }

    TransactionSlotPtr GetTrxSlotLocation()
    {
        return GenerateTrxSlotPtr(undo_segment->MyId(), trxslot_id);
    }

    UndoRecPtr InsertUndoRecord(UndoRecord *record);
    inline void RollBack(char* undo_record_cache)
    {
        undo_segment->RollBack(trxslot, undo_record_cache);
    }
};

}

#endif // NVMDB_UNDO_CONTEXT_H