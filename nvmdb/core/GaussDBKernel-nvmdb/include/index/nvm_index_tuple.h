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
 * nvm_index_tuple.h
 *
 * IDENTIFICATION
 *   src/gausskernel/storage/nvmdb/core/GaussDBKernel-nvmdb/include/index/nvm_index_tuple.h
 * -------------------------------------------------------------------------
 */
#ifndef NVMDB_INDEX_TUPLE_H
#define NVMDB_INDEX_TUPLE_H

#include "nvm_types.h"
#include "pactree.h"
#include "codec/nvm_codec.h"

namespace NVMDB {

typedef struct IndexColumnDesc {
    uint32 m_colId;
    uint64 m_colLen;
    uint64 m_colOffset;
} IndexColumnDesc;

typedef struct IndexDesc {
    IndexColumnDesc *m_indexColDesc = nullptr;
    uint32 m_indexColCnt;
    uint64 m_indexLen;
} IndexDesc;

void InitIndexDesc(IndexColumnDesc *const indexDes, const ColumnDesc *const rowDes, const uint32 &colCnt,
                   uint64 &indexLen);

inline bool IsIndexTypeSupported(ColumnType indexType)
{
    bool ret = false;
    if (indexType == COL_TYPE_INT || indexType == COL_TYPE_UNSIGNED_LONG || indexType == COL_TYPE_VARCHAR) {
        ret = true;
    }
    return ret;
}

/* 内存中的 IndexTuple */
class DRAMIndexTuple {
public:
    const ColumnDesc *const m_rowDes;
    const IndexColumnDesc *const m_indexDes;
    const uint32 m_colCnt;
    char *m_indexData;
    uint64 m_indexLen;

    DRAMIndexTuple(const ColumnDesc *rowDes, const IndexColumnDesc *indexDes, uint32 colCnt, uint64 indexLen)
        : m_rowDes(rowDes), m_indexDes(indexDes), m_colCnt(colCnt), m_indexLen(indexLen)
    {
        Assert(indexLen <= MAX_TUPLE_LEN);
        m_indexData = new char[indexLen];
        int ret = memset_s(m_indexData, indexLen, 0, indexLen);
        SecureRetCheck(ret);
    }

    ~DRAMIndexTuple()
    {
        delete[] m_indexData;
    }

    void ExtractFromTuple(RAMTuple *tuple)
    {
        Assert(m_rowDes == tuple->m_rowDes);
        const char *const rowData = tuple->m_rowData;
        uint64 offset = 0;
        uint64 colLen = 0;
        uint32 colId = 0;
        for (uint32 i = 0; i < m_colCnt; i++) {
            colId = m_indexDes[i].m_colId;
            const char *const colData = &rowData[m_rowDes[colId].m_colOffset];
            colLen = m_rowDes[colId].m_colLen;
            int ret = memcpy_s(m_indexData + offset, m_indexLen - offset, colData, colLen);
            SecureRetCheck(ret);
            offset += colLen;
        }
        Assert(offset == m_indexLen);
    }

    inline void SetCol(const uint32 indexColId, const char *const colData, bool isVarChar = false)
    {
        Assert(indexColId < m_colCnt);
        if (isVarChar) {
            int ret =
                strcpy_s(m_indexData + m_indexDes[indexColId].m_colOffset, m_indexDes[indexColId].m_colLen, colData);
            SecureRetCheck(ret);
        } else {
            int ret = memcpy_s(m_indexData + m_indexDes[indexColId].m_colOffset, m_indexDes[indexColId].m_colLen,
                               colData, m_indexDes[indexColId].m_colLen);
            SecureRetCheck(ret);
        }
    }

    inline void SetCol(const uint32 indexColId, const char *const colData, uint64 len)
    {
        Assert(indexColId < m_colCnt);
        Assert(m_indexDes[indexColId].m_colLen >= len);
        int ret =
            memcpy_s(m_indexData + m_indexDes[indexColId].m_colOffset, m_indexDes[indexColId].m_colLen, colData, len);
        SecureRetCheck(ret);
    }

    inline void FillColWith(const uint32 indexColId, char data, uint64 len)
    {
        Assert(indexColId < m_colCnt);
        Assert(m_indexDes[indexColId].m_colLen >= len);
        int ret =
            memset_s(m_indexData + m_indexDes[indexColId].m_colOffset, m_indexDes[indexColId].m_colLen, data, len);
        SecureRetCheck(ret);
    }

    inline char *GetCol(const uint32 indexColId) const
    {
        return m_indexData + m_indexDes[indexColId].m_colOffset;
    }

    static char *EncodeInt32Wrap(char *buf, int32 i, int &len)
    {
        *buf = CODE_INT32;
        buf++;
        EncodeInt32(buf, i);
        buf += sizeof(int32);
        len += 1 + sizeof(int32);
        return buf;
    }

    static char *EncodeUint64Wrap(char *buf, uint64 u, int &len)
    {
        *buf = CODE_UINT64;
        buf++;
        EncodeUint64(buf, u);
        buf += sizeof(uint64);
        len += 1 + sizeof(uint64);
        return buf;
    }

    static char *EncodeVarcharWrap(char *buf, char *data, int strlen, int &len)
    {
        *buf = CODE_VARCHAR;
        buf++;
        EncodeVarchar(buf, data, strlen);
        buf += strlen + 1;
        len += 1 + strlen + 1;
        return buf;
    }

    int Encode(char *buf)
    {
        int len = 0;
        uint32 colId = 0;
        uint64 colLen = 0;
        uint64 offset = 0;
        char *colData = nullptr;
        for (uint32 i = 0; i < m_colCnt; i++) {
            colId = m_indexDes[i].m_colId;
            colLen = m_rowDes[colId].m_colLen;
            colData = m_indexData + offset;
            switch (m_rowDes[colId].m_colType) {
                case COL_TYPE_INT: {
                    buf = EncodeInt32Wrap(buf, *(int32 *)colData, len);
                    break;
                }
                case COL_TYPE_UNSIGNED_LONG: {
                    buf = EncodeUint64Wrap(buf, *(uint64 *)colData, len);
                    break;
                }
                case COL_TYPE_VARCHAR: {
                    uint32 varLen = *((uint32 *)colData);
                    buf = EncodeVarcharWrap(buf, colData + sizeof(uint32), varLen, len);
                    break;
                }
                default: {
                    ALWAYS_CHECK(false);
                }
            }
            offset += colLen;
        }
        Assert(offset == m_indexLen);
        return len;
    }

    void Copy(const DRAMIndexTuple *tuple)
    {
        Assert(m_rowDes == tuple->m_rowDes);
        Assert(m_indexDes == tuple->m_indexDes);

        int ret = memcpy_s(m_indexData, m_indexLen, tuple->m_indexData, m_indexLen);
        SecureRetCheck(ret);

        return;
    }

    void Copy(const DRAMIndexTuple *tuple, uint32 colIndex)
    {
        Assert(m_rowDes == tuple->m_rowDes);
        Assert(m_indexDes == tuple->m_indexDes);
        Assert(colIndex < tuple->m_colCnt);

        int ret = memcpy_s(m_indexData + m_indexDes[colIndex].m_colOffset, m_indexDes[colIndex].m_colLen,
                           tuple->m_indexData + m_indexDes[colIndex].m_colOffset, m_indexDes[colIndex].m_colLen);
        SecureRetCheck(ret);

        return;
    }
};

}  // namespace NVMDB

#endif  // NVMDB_INDEX_TUPLE_H
