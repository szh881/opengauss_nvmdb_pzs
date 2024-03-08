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
 * nvm_undo_record.cpp
 *
 * IDENTIFICATION
 *   src/gausskernel/storage/nvmdb/core/GaussDBKernel-nvmdb/dbcore/undo/nvm_undo_record.cpp
 * -------------------------------------------------------------------------
 */
#include <cstring>

#include "nvm_undo_internal.h"
#include "nvm_undo_segment.h"
#include "nvm_undo_record.h"

namespace NVMDB {

UndoRecord *CopyUndoRecord(UndoRecPtr ptr, char* undo_record_cache)
{
    int segid = UndoRecPtrGetSegment(ptr);
    UndoSegment *undo_segment = GetUndoSegment(segid);
    return undo_segment->CopyUndoRecord(ptr, undo_record_cache);
}

}
