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
 * nvm_tuple.cpp
 *
 * IDENTIFICATION
 *   src/gausskernel/storage/nvmdb/core/GaussDBKernel-nvmdb/dbcore/heap/nvm_tuple.cpp
 * -------------------------------------------------------------------------
 */
#include <cstring>

#include "nvm_undo_record.h"
#include "nvm_heap_undo.h"
#include "nvm_tuple.h"

namespace NVMDB {

void RAMTuple::Serialize(char *buf, size_t bufLen)
{
    NVMTuple head = {.m_trxInfo = m_trxInfo,
                     .m_prev = m_prev,
                     .m_flag1 = m_flag1,
                     .m_flag2 = m_flag2,
                     .m_len = m_len,
                     .m_null = m_isNullBitmap.to_ulong()};
    int ret = memcpy_s(buf, bufLen, &head, NVMTupleHeadSize);
    SecureRetCheck(ret);
    ret = memcpy_s(buf + NVMTupleHeadSize, bufLen - NVMTupleHeadSize, m_rowData, m_rowLen);
    SecureRetCheck(ret);
}

void RAMTuple::Deserialize(char *nvmTuple)
{
    NVMTuple head;
    int ret = memcpy_s(&head, sizeof(head), nvmTuple, NVMTupleHeadSize);
    SecureRetCheck(ret);
    m_trxInfo = head.m_trxInfo;
    m_prev = head.m_prev;
    m_flag1 = head.m_flag1;
    m_flag2 = head.m_flag2;
    m_len = head.m_len;
    m_isNullBitmap = head.m_null;

    ret = memcpy_s(m_rowData, m_rowLen, nvmTuple + NVMTupleHeadSize, m_rowLen);
    SecureRetCheck(ret);
}

bool RAMTuple::HasPreVersion()
{
    return !UndoRecPtrIsInValid(m_prev);
}

void RAMTuple::FetchPreVersion(char *undoRecordCache)
{
    Assert(!UndoRecPtrIsInValid(m_prev));
    UndoRecord *undo = CopyUndoRecord(m_prev, undoRecordCache);
    if (undo->m_undoType == HeapUpdateUndo) {
        UndoUpdate(undo, this);
    } else {
        Deserialize(undo->data);
    }
}

bool RAMTuple::IsInUsed()
{
    return m_flag1 & NVMTUPLE_USED;
}

}  // namespace NVMDB