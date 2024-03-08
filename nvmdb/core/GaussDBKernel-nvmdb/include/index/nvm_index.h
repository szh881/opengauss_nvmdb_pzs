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
 * nvm_index.h
 *
 * IDENTIFICATION
 *   src/gausskernel/storage/nvmdb/core/GaussDBKernel-nvmdb/include/index/nvm_index.h
 * -------------------------------------------------------------------------
 */
#ifndef NVMDB_INDEX_H
#define NVMDB_INDEX_H

#include "nvm_types.h"
#include "nvm_index_tuple.h"
#include "pactree.h"
#include "codec/nvm_codec.h"
#include "nvm_index_iter.h"

namespace NVMDB {

/* idx id + tag + row id */
static constexpr uint32 KEY_EXTRA_LENGTH = sizeof(uint32)  + 1  + sizeof(uint32) ;
static constexpr uint32 KEY_DATA_LENGTH = KEYLENGTH - KEY_EXTRA_LENGTH;

void IndexBootstrap(const char *dir);
void IndexExitProcess();
void InitLocalIndex(int grpId);
void DestroyLocalIndex();
IndexColumnDesc *IndexDescCreate(uint32 colCount);
void IndexDescDelete(IndexColumnDesc *desc);

class NVMIndex {
    TableId m_idxId;
    uint32 m_colCnt = 0;
    uint64 m_rowLen = 0;
    IndexColumnDesc *m_indexDes = nullptr;
    uint8 *m_colBitmap = nullptr;

public:
    NVMIndex(TableId id) : m_idxId(id)
    {}

    ~NVMIndex()
    {
        delete[] m_colBitmap;
        IndexDescDelete(m_indexDes);
    }

    void Encode(DRAMIndexTuple *tuple, Key_t *key, RowId rowId)
    {
        char *data = key->getData();
        EncodeUint32(data, m_idxId);
        data += sizeof(uint32);
        int len = tuple->Encode(data);
        data += len;
        *data = CODE_ROWID;
        EncodeUint32(data + 1, rowId);
        key->keyLength = KEY_EXTRA_LENGTH + len;
        Assert(key->keyLength <= KEYLENGTH);
    }

    /* find begin <= key <= end */
    NVMIndexIter *GenerateIter(DRAMIndexTuple *begin, DRAMIndexTuple *end, LookupSnapshot snapshot, int max_range,
                               bool reverse)
    {
        Key_t kb;
        Key_t ke;
        Encode(begin, &kb, 0);
        Encode(end, &ke, 0xffffffff);

        auto iter = new NVMIndexIter(kb, ke, snapshot, max_range, reverse);
        return iter;
    }

    void Insert(DRAMIndexTuple *tuple, RowId rowId)
    {
        pactree *pt = GetGlobalPACTree();
        Key_t key;
        Encode(tuple, &key, rowId);
        pt->Insert(key, INVALID_CSN);
    }

    void Delete(DRAMIndexTuple *tuple, RowId rowId, TransactionSlotPtr trx)
    {
        pactree *pt = GetGlobalPACTree();
        Key_t key;
        Encode(tuple, &key, rowId);
        pt->Insert(key, trx);
    }

    bool SetNumTableFields(uint32 num)
    {
        Assert(num <= NVMDB_TUPLE_MAX_COL_COUNT);
        bool result = false;
        m_colBitmap = new uint8[BITMAP_GETLEN(num)]{0};
        Assert(m_colBitmap != nullptr);
        return m_colBitmap != nullptr;
    }

    void FillBitmap(const uint32 colID) noexcept
    {
        if (m_colBitmap != nullptr) {
            BITMAP_SET(m_colBitmap, colID);
        } else {
            Assert(false);
        }
    }

    bool IsFieldPresent(uint32 colid) const noexcept
    {
        Assert(m_colBitmap != nullptr);
        return BITMAP_GET(m_colBitmap, colid);
    }

    uint32 GetColCount() const noexcept
    {
        return m_colCnt;
    }

    uint32 GetRowLen() const noexcept
    {
        return m_rowLen;
    }

    void SetIndexDesc(IndexColumnDesc *desc, uint32 colCount, uint64 rowLen) noexcept
    {
        m_indexDes = desc;
        m_colCnt = colCount;
        m_rowLen = rowLen;
    }

    const IndexColumnDesc *GetIndexDesc() const noexcept
    {
        return m_indexDes;
    }

    TableId Id() const
    {
        return m_idxId;
    }
};

}  // namespace NVMDB

#endif  // NVMDB_INDEX_H
