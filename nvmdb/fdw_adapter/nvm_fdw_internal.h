/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
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
 */
#ifndef AUSSKERNEL_STORAGE_NVM_ADAPTER_NVM_FDW_INTERNAL_H
#define AUSSKERNEL_STORAGE_NVM_ADAPTER_NVM_FDW_INTERNAL_H

#include "foreign/fdwapi.h"
#include "nodes/nodes.h"
#include "nodes/makefuncs.h"
#include "utils/numeric.h"
#include "utils/numeric_gs.h"
#include "pgstat.h"
#include "nvm_types.h"
#include "nvm_fdw_iter.h"
#include "nvm_index.h"
#include "nvm_table.h"

constexpr char *NVM_REC_TID_NAME = "ctid";

enum class NVM_ERRCODE {
    NVM_SUCCESS = 0,
    NVM_ERRCODE_INPUT_PARA_ERROR,
    NVM_ERRCODE_UNSUPPORTED_COL_TYPE,
    NVM_ERRCODE_NO_MEM,
    NVM_ERRCODE_TABLE_NOT_FOUND,
    NVM_ERRCODE_INDEX_NOT_FOUND,
    NVM_ERRCODE_COL_NOT_FOUND,
    NVM_ERRCODE_INDEX_TYPE_NOT_SUPPORT,
    NVM_ERRCODE_INDEX_NOT_SUPPORT_NULL,
    NVM_ERRCODE_INDEX_SIZE_EXC_LIMIT,
    NVM_ERRCODE_COL_COUNT_EXC_LIMIT,
    NVM_ERRCODE_INDEX_NOT_SUPPORT_EXPR,
    NVM_ERRCODE_INVALID
};
enum class SORTDIR_ENUM {
    SORTDIR_NONE = 0,
    SORTDIR_ASC = 1,
    SORTDIR_DESC = 2,
};
enum class FDW_LIST_TYPE {
    FDW_LIST_STATE = 1,
    FDW_LIST_BITMAP = 2,
};

enum class NVM_ITER_TYPE {
    NVM_ITER_TYPE_INVALID = 0,
    NVM_ITER_TYPE_SEQ = 0x11,
    NVM_ITER_TYPE_INDEX = 0x22,
};

constexpr uint64 MAX_VARCHAR_LEN = 1024;
// Decimal representation
constexpr int DECIMAL_POSITIVE = 0x01;
constexpr int DECIMAL_NEGATIVE = 0x02;
constexpr int DECIMAL_NAN = 0x04;
constexpr int DECIMAL_DIGITS_PTR = 0x08;
constexpr int DECIMAL_MAX_DIGITS = 16;
constexpr int NVM_NUMERIC_MAX_PRECISION = DECIMAL_MAX_DIGITS * 4;

struct __attribute__((packed)) DecimalHdrSt {
    uint8 m_flags;
    uint16 m_weight;
    uint16 m_scale;
    uint16 m_ndigits;
};

struct __attribute__((packed)) DecimalSt {
    DecimalHdrSt m_hdr;
    uint16 m_round;
    uint16 m_digits[0];
};

#define DECIMAL_MAX_SIZE (sizeof(DecimalSt) + DECIMAL_MAX_DIGITS * sizeof(uint16))
#define DECIMAL_SIZE(d) (sizeof(DecimalSt) + (d)->m_hdr.m_ndigits * sizeof(int16))

struct __attribute__((packed)) IntervalSt {
    uint64 m_time;
    int32 m_day;
    int32 m_month;
};

struct __attribute__((packed)) TimetzSt {
    uint64 m_time;
    int32 m_zone;
};

struct __attribute__((packed)) TintervalSt {
    int32 m_status;
    int32 m_data[2];
};

struct NVMFdwConstraint {
    NVMDB::NVMIndex *mIndex;
    NVMDB::KEY_OPER mOper[NVM_MAX_KEY_COLUMNS];
    uint32 mMatchCount;
    List *mParentExprList;
    List *mExprList;
    double mCost;
    double mStartupCost;
};

struct NVMFdwState_St {
    CmdType mCmdOper;
    bool mAllocInScan;
    Oid mForeignTableId;
    AttrNumber mNumAttrs;
    AttrNumber mCtidNum;
    uint16_t mNumExpr;
    uint8 *mAttrsUsed;
    uint8 *mAttrsModified;
    List *mLocalConds;
    NVMFdwConstraint mConst;
    NVMFdwConstraint mConstPara;
    NVMDB::Table *mTable;
    NVMDB::Transaction *mCurrTxn;
    NVMDB::RowId mRowIndex;
    bool mCursorOpened;
    NVMDB::NvmFdwIter *mIter;
};

#endif
