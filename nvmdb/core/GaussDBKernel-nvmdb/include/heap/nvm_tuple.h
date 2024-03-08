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
 * nvm_tuple.h
 *
 * IDENTIFICATION
 *   src/gausskernel/storage/nvmdb/core/GaussDBKernel-nvmdb/include/heap/nvm_tuple.h
 * -------------------------------------------------------------------------
 */
#ifndef NVMDB_TUPLE_H
#define NVMDB_TUPLE_H

#include "nvm_undo_ptr.h"
#include "securec.h"
#include "nvm_cfg.h"
#include <bitset>

namespace NVMDB {

static constexpr uint32 NVMDB_TUPLE_MAX_COL_COUNT = 64;
static constexpr uint32 NVM_MAX_COLUMN_NAME_LEN = 84;

typedef enum ColumnType : uint32 {
    COL_TYPE_INT = 0,
    COL_TYPE_LONG = 1,
    COL_TYPE_FLOAT = 2,
    COL_TYPE_VARCHAR = 3,
    COL_TYPE_UNSIGNED_LONG = 4,
    /* above required in tpc-c. */
    COL_TYPE_DOUBLE = 5,
    COL_TYPE_SHORT = 6,
    COL_TYPE_TINY = 7,
    COL_TYPE_DATE = 8,
    COL_TYPE_TIME = 9,
    COL_TYPE_CHAR = 10,
    COL_TYPE_TIMESTAMP = 11,
    COL_TYPE_TIMESTAMPTZ = 12,
    COL_TYPE_INTERVAL = 13,
    COL_TYPE_TINTERVAL = 14,
    COL_TYPE_TIMETZ = 15,
    COL_TYPE_DECIMAL = 16,
    COL_TYPE_INVALID
} ColumnType;

struct ColumnDesc {
    ColumnType m_colType;
    uint32 m_colOid;
    uint64 m_colLen;
    uint64 m_colOffset;
    bool m_isNotNull;
    char m_colName[NVM_MAX_COLUMN_NAME_LEN];
};

inline void InitColumnDesc(ColumnDesc *const rowDes, const uint32 &colCnt, uint64 &rowLen)
{
    Assert(rowDes != nullptr && colCnt != 0);
    uint64 offset = 0;
    for (uint32 i = 0; i < colCnt; i++) {
        rowDes[i].m_colOffset = offset;
        offset += rowDes[i].m_colLen;
    }
    rowLen = offset;
}

struct ColumnUpdate {
    uint32 m_colId = 0;
    char *m_colData = nullptr;
};

struct UndoColumnDesc {
    uint64 m_colOffset;
    uint64 m_colLen;
};

struct UndoUpdatePara {
    UndoColumnDesc *m_updatedCols{nullptr};
    uint32 m_updateCnt{0};
    uint64 m_updateLen{0};
};

using NvmNullType = std::bitset<NVMDB_TUPLE_MAX_COL_COUNT>;

struct NVMTuple {
    /*
     * 事务状态信息，如果事务已经提交，是csn；如果正在执行，是TransactionSlotPtr (TSP)的位置信息。
     * 这里利用了 TSP 只有32位，所以首bit肯定是0。同时设计最小的 CSN 为 1<<63, CSN 的首bit肯定是1.
     * 所以可以用第一个bit区分是CSN 还是 TSP
     * */
    uint64 m_trxInfo;
    UndoRecPtr m_prev;   /* 指向undo区域的旧版本 */
    uint32 m_flag1;      /* 保留位，32个bit的 flag */
    uint32 m_flag2 : 16; /* 保留位，16个bit 的falg */
    uint32 m_len : 16;   /* tuple 中数据的长度 */
    uint64 m_null{0};
    static_assert(BIS_PER_BYTE * sizeof(m_null) >= NVMDB_TUPLE_MAX_COL_COUNT);
    char data[0];
};

/* 一般情况下，TransactionSlot 和 CSN 都会共有一个变量，如 tuple 里的trx_info，index 里的 value，
 * 所以需要在范围上予以区分。TransactionSlot 的首bit肯定是0，因此以 1<<63 为分界线。
 */
#define MIN_TRX_CSN ((1LLU << 63) + 1)
#define INVALID_CSN (1LLU << 63)
#define MAX_TRX_CSN (~0LLU)

inline bool TrxInfoIsCsn(uint64 trxInfo)
{
    return trxInfo >= MIN_TRX_CSN;
}

inline bool TrxInfoIsTrxSlot(uint64 trxInfo)
{
    return trxInfo < INVALID_CSN;
}

class RAMTuple : public NVMTuple {
public:
    const ColumnDesc *const m_rowDes;
    UndoColumnDesc *m_updatedCols;
    uint32 m_updateCnt;
    uint64 m_updateLen;
    bool m_isNewSelf = true;
    char *m_rowData;
    uint64 m_rowLen;
    NvmNullType m_isNullBitmap;

    RAMTuple(const ColumnDesc *rowDes, uint64 rowLen)
        : m_rowDes(rowDes), m_rowLen(rowLen), m_updateCnt(0), m_updateLen(0)
    {
        Assert(rowLen <= MAX_TUPLE_LEN);
        m_rowData = new char[rowLen];
        int ret = memset_s(m_rowData, rowLen, 0, rowLen);
        SecureRetCheck(ret);
        m_updatedCols = new UndoColumnDesc[rowLen];
    }

    RAMTuple(const ColumnDesc *rowDes, uint64 rowLen, char *rowData, UndoColumnDesc *updatedCols) noexcept
        : m_rowDes(rowDes),
          m_rowLen(rowLen),
          m_rowData(rowData),
          m_updatedCols(updatedCols),
          m_isNewSelf(false),
          m_updateCnt(0),
          m_updateLen(0)
    {}

    ~RAMTuple()
    {
        if (m_isNewSelf) {
            delete[] m_rowData;
            delete[] m_updatedCols;
        }
    }

    void UpdateCols(ColumnUpdate *updates, uint32 update_cnt)
    {
        Assert(updates != nullptr && update_cnt != 0);
        m_updateCnt = update_cnt;
        m_updateLen = 0;
        uint32 colId = 0;
        uint64 colOffset = 0;
        uint64 colLen = 0;
        for (uint32 i = 0; i < update_cnt; i++) {
            colId = updates[i].m_colId;
            colOffset = m_rowDes[colId].m_colOffset;
            colLen = m_rowDes[colId].m_colLen;
            m_updateLen += colLen;
            m_updatedCols[i].m_colOffset = colOffset;
            m_updatedCols[i].m_colLen = colLen;
            int ret = memcpy_s(m_rowData + colOffset, m_rowLen - colOffset, updates[i].m_colData, colLen);
            SecureRetCheck(ret);
        }
    }

    inline void UpdateCol(const uint32 colId, char *const colData)
    {
        ColumnUpdate _updates{colId, colData};
        UpdateCols(&_updates, 1);
    }

    inline void UpdateColInc(const uint32 colId, char *const colData, uint64 len)
    {
        uint64 colOffset = 0;
        uint64 colLen = 0;
        colOffset = m_rowDes[colId].m_colOffset;
        colLen = m_rowDes[colId].m_colLen;
        m_updateLen += colLen;
        m_updatedCols[m_updateCnt].m_colOffset = colOffset;
        m_updatedCols[m_updateCnt].m_colLen = colLen;
        int ret = memcpy_s(m_rowData + colOffset, m_rowLen - colOffset, colData, len);
        SecureRetCheck(ret);

        m_updateCnt++;
    }

    inline void GetUpdatedCols(UndoColumnDesc *&updatedCols, uint32 &updateCnt, uint64 &updateLen) const
    {
        updatedCols = m_updatedCols;
        updateCnt = m_updateCnt;
        updateLen = m_updateLen;
    }

    void SetCols(ColumnUpdate *updates, uint32 updateCnt)
    {
        Assert(updates != nullptr && updateCnt != 0);
        uint32 colId = 0;
        for (uint32 i = 0; i < updateCnt; i++) {
            colId = updates[i].m_colId;
            int ret = memcpy_s(m_rowData + m_rowDes[colId].m_colOffset, m_rowLen - m_rowDes[colId].m_colOffset,
                               updates[i].m_colData, m_rowDes[colId].m_colLen);
            SecureRetCheck(ret);
        }
    }

    void GetCols(ColumnUpdate *updates, uint32 updateCnt) const
    {
        Assert(updates != nullptr && updateCnt != 0);
        uint32 col_id = 0;
        for (uint32 i = 0; i < updateCnt; i++) {
            col_id = updates[i].m_colId;
            int ret = memcpy_s(updates[i].m_colData, m_rowDes[col_id].m_colLen,
                               m_rowData + m_rowDes[col_id].m_colOffset, m_rowDes[col_id].m_colLen);
            SecureRetCheck(ret);
        }
    }

    inline void SetCol(const uint32 colId, char *const colData)
    {
        int ret = memcpy_s(m_rowData + m_rowDes[colId].m_colOffset, m_rowLen - m_rowDes[colId].m_colOffset, colData,
                           m_rowDes[colId].m_colLen);
        SecureRetCheck(ret);
    }

    inline void SetCol(const uint32 colId, char *const colData, uint64 len)
    {
        int ret =
            memcpy_s(m_rowData + m_rowDes[colId].m_colOffset, m_rowLen - m_rowDes[colId].m_colOffset, colData, len);
        SecureRetCheck(ret);
    }

    inline void GetCol(const uint32 colId, char *const colData) const
    {
        int ret = memcpy_s(colData, m_rowDes[colId].m_colLen, m_rowData + m_rowDes[colId].m_colOffset,
                           m_rowDes[colId].m_colLen);
        SecureRetCheck(ret);
    }

    inline char *GetCol(const uint32 colId) const
    {
        return m_rowData + m_rowDes[colId].m_colOffset;
    }

    inline void CopyRow(RAMTuple *tuple)
    {
        Assert(tuple->m_rowLen == m_rowLen);
        int ret = memcpy_s(m_rowData, m_rowLen, tuple->m_rowData, m_rowLen);
        SecureRetCheck(ret);
    }

    inline bool EqualRow(RAMTuple *tuple) const
    {
        return (memcmp(m_rowData, tuple->m_rowData, m_rowLen) == 0);
    }

    inline bool ColEqual(const uint32 colId, char *const colData) const
    {
        return (memcmp(m_rowData + m_rowDes[colId].m_colOffset, colData, m_rowDes[colId].m_colLen) == 0);
    }

    void InitHead(uint64 trxInfo, UndoRecPtr prev, uint32 flag1, uint16 flag2)
    {
        m_trxInfo = trxInfo;
        m_prev = prev;
        m_flag1 = flag1;
        m_flag2 = flag2;
        m_len = m_rowLen;
    }

    bool HasPreVersion();
    void FetchPreVersion(char *undoRecordCache);
    void Serialize(char *buf, size_t bufLen);
    void Deserialize(char *buf);
    bool IsInUsed();
    inline bool TrxInfoIsCSN()
    {
        return TrxInfoIsCsn(m_trxInfo);
    }

    inline uint16 payload()
    {
        return m_rowLen;
    }

    inline bool IsNull(uint32 colId) const
    {
        return m_isNullBitmap[colId];
    }

    inline void SetNull(uint32 colOd, bool isNull = true)
    {
        m_isNullBitmap.set(colOd, isNull);
    }
};

static const int NVMTupleHeadSize = sizeof(NVMTuple);

inline size_t RealTupleSize(size_t row_len)
{
    return row_len + NVMTupleHeadSize;
}

#define NVMTUPLE_USED 0x00000001    /* the tuple is used */
#define NVMTUPLE_DELETED 0x00000002 /* the tuple is deleted */

static inline bool NVMTupleIsUsed(NVMTuple *tuple)
{
    return tuple->m_flag1 & NVMTUPLE_USED;
}

static inline void NVMTupleSetUsed(NVMTuple *tuple)
{
    tuple->m_flag1 |= NVMTUPLE_USED;
}

static inline void NVMTupleSetUnUsed(NVMTuple *tuple)
{
    tuple->m_flag1 &= ~NVMTUPLE_USED;
}

static inline void NVMTupleSetDeleted(NVMTuple *tuple)
{
    tuple->m_flag1 |= NVMTUPLE_DELETED;
}

static inline bool NVMTupleDeleted(NVMTuple *tuple)
{
    return tuple->m_flag1 & NVMTUPLE_DELETED;
}

NVMTuple *AssembleNVMTuple(const char *data, int len);
void ReleaseNVMTuple(NVMTuple *tuple);

}  // namespace NVMDB

#endif // NVMDB_TUPLE_H