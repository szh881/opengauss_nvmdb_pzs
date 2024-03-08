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
 * nvm_undo_rollback.cpp
 *
 * IDENTIFICATION
 *   src/gausskernel/storage/nvmdb/core/GaussDBKernel-nvmdb/dbcore/undo/nvm_undo_rollback.cpp
 * -------------------------------------------------------------------------
 */
#include <cstring>

#include "nvm_undo_record.h"
#include "nvm_heap_undo.h"
#include "index/nvm_index_undo.h"
#include "nvm_undo_rollback.h"

namespace NVMDB {

using NVMUndoFunc = void (*)(UndoRecord *);

struct NVMUndoProcedure {
    UndoRecordType type;
    std::string name;
    NVMUndoFunc undoFunc;
};

static NVMUndoProcedure g_nvmUndoFuncs[] = {
    {InvalidUndoRecordType, "", NULL},
    {HeapInsertUndo, "HeapInsertUndo", UndoInsert},
    {HeapUpdateUndo, "HeapUpdateUndo", UndoUpdate},
    {HeapDeleteUndo, "HeapDeleteUndo", UndoDelete},
    {IndexInsertUndo, "IndexInsertUndo", UndoIndexInsert},
    {IndexDeleteUndo, "IndexDeleteUndo", UndoIndexDelete},
};

void UndoRecordRollBack(UndoRecord *record)
{
    if (UndoRecordTypeIsValid((UndoRecordType)record->m_undoType)) {
        NVMUndoProcedure *procedure = &g_nvmUndoFuncs[record->m_undoType];
        Assert(procedure->type == record->m_undoType);
        procedure->undoFunc(record);
    }
}

}