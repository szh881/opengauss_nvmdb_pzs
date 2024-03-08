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
 * nvm_vecstore.cpp
 *
 * IDENTIFICATION
 *   src/gausskernel/storage/nvmdb/core/GaussDBKernel-nvmdb/dbcore/heap/nvm_vecstore.cpp
 * -------------------------------------------------------------------------
 */
#include <mutex>

#include "nvmdb_thread.h"
#include "nvm_tuple.h"
#include "heap/nvm_rowid_mgr.h"
#include "nvm_vecstore.h"

namespace NVMDB {

VecStore::VecStore(TableSpace *tblspc, uint32 seghead, uint32 rowLen)
{
    m_tblspc = tblspc;
    m_seghead = seghead;
    m_tupleLen = rowLen + NVMTupleHeadSize;
    m_tuplesPerpage = PageContentSize(HEAP_EXTENT_SIZE) / m_tupleLen;

    uint32 pagesPerDir = MaxRowId / m_tuplesPerpage / g_dirPathNum;
    m_gbm = new GlobalBitMap *[g_dirPathNum];
    Assert(g_dirPathNum > 0);
    for (uint32 i = 0; i < g_dirPathNum; i++) {
        m_gbm[i] = new GlobalBitMap(pagesPerDir);
    }
    m_rowidMgr = new RowIDMgr(m_tblspc, m_seghead, m_tupleLen);
}

VecStore::~VecStore()
{
    for (uint32 i = 0; i < g_dirPathNum; i++) {
        delete m_gbm[i];
    }
    delete[] m_gbm;
}

char *VecStore::TryAt(RowId rid)
{
    char *tuple = m_rowidMgr->version_pointer(rid, true);
    NVMTuple *tupleHead = reinterpret_cast<NVMTuple *>(tuple);
    if (NVMTupleIsUsed(tupleHead)) {
        return nullptr;
    }
    return tuple;
}

RowId VecStore::TryNextRowid()
{
    ThreadLocalTableCache *localTableCache = GetThreadLocalTableCache(m_seghead);
    RowId rid = InvalidRowId;

    // 1. 从 RowID Cache 中找，是否有自己之前删过的。
    rid = localTableCache->m_rowidCache.pop();
    if (RowIdIsValid(rid)) {
        return rid;
    }

    while (true) {
        // 2. 从 Range 中找从来没有用过的。
        rid = localTableCache->m_range.next();
        if (RowIdIsValid(rid)) {
            return rid;
        }

        // 3. 从 GlobalBitMap中分配一个新的Range
        uint32 dirSeq = GetCurrentGroupId() % g_dirPathNum;
        uint32 bit = m_gbm[dirSeq]->SyncAcquire();
        bit = dirSeq + g_dirPathNum * bit;
        localTableCache->m_range.start = bit * m_tuplesPerpage;
        localTableCache->m_range.end = (bit + 1) * m_tuplesPerpage;
    }

    assert(0);
}

char *VecStore::VersionPoint(RowId rowId)
{
    char *res = m_rowidMgr->version_pointer(rowId, false);
    return res;
}

RowId VecStore::InsertVersion()
{
    while (true) {
        RowId rowId = TryNextRowid();
        char *pointer = TryAt(rowId);
        if (pointer != nullptr) {
            return rowId;
        }
    }
}

RowId VecStore::GetUpperRowId()
{
    RowId nvmRowId = m_rowidMgr->GetUpperRowId();
    return nvmRowId;
}

}  // namespace NVMDB