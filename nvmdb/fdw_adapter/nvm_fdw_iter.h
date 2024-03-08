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

#ifndef AUSSKERNEL_STORAGE_NVM_ADAPTER_NVM_FDW_ITER_H
#define AUSSKERNEL_STORAGE_NVM_ADAPTER_NVM_FDW_ITER_H

#include "nvm_types.h"
#include "nvm_index.h"
#include "nvm_index_iter.h"
#include "foreign/fdwapi.h"
#include "nodes/nodes.h"
#include "nodes/makefuncs.h"
#include "utils/numeric.h"
#include "utils/numeric_gs.h"
#include "pgstat.h"

constexpr int NVM_MAX_KEY_COLUMNS = 10U;

using NVMFdwState = struct NVMFdwState_St;

namespace NVMDB {

enum class KEY_OPER : uint8_t {
    READ_KEY_EXACT = 0,    // equal
    READ_KEY_OR_NEXT = 1,  // ge
    READ_KEY_AFTER = 2,    // gt
    READ_KEY_OR_PREV = 3,  // le
    READ_KEY_BEFORE = 4,   // lt
    READ_INVALID = 5,
};

class NvmFdwIter {
public:
    virtual void Next() = 0;
    virtual bool Valid() = 0;
    virtual RowId GetRowId() = 0;
    virtual ~NvmFdwIter()
    {}
};

class NvmFdwIndexIter : public NvmFdwIter {
public:
    void Next() override
    {
        m_indexIter->Next();
    }
    bool Valid() override
    {
        return m_indexIter->Valid();
    }
    RowId GetRowId() override
    {
        return m_indexIter->Curr();
    }

    explicit NvmFdwIndexIter(NVMIndexIter *iter) noexcept : m_indexIter(iter)
    {
        assert(m_indexIter != nullptr);
    }

    ~NvmFdwIndexIter() override
    {
        delete m_indexIter;
    }

private:
    NVMIndexIter *m_indexIter;
};

class NvmFdwSeqIter : public NvmFdwIter {
public:
    void Next() override
    {
        m_curRowId++;
    }

    bool Valid() override
    {
        return m_curRowId < m_maxRowId;
    }

    RowId GetRowId() override
    {
        return m_curRowId;
    }

    explicit NvmFdwSeqIter(RowId max) noexcept : m_maxRowId(max)
    {}

private:
    RowId m_curRowId = 0;
    const RowId m_maxRowId;
};

class NvmMatchIndex {
public:
    NvmMatchIndex() noexcept
    {
        Init();
    }

    ~NvmMatchIndex() noexcept
    {}

    List *m_remoteConds = nullptr;
    List *m_remoteCondsOrig = nullptr;
    NVMIndex *m_ix = nullptr;
    uint32 m_ixPosition = 0;
    Expr *m_colMatch[NVM_MAX_KEY_COLUMNS];
    Expr *m_parentColMatch[NVM_MAX_KEY_COLUMNS];
    KEY_OPER m_opers[NVM_MAX_KEY_COLUMNS];
    uint32 m_params[NVM_MAX_KEY_COLUMNS];
    uint32 m_col[NVM_MAX_KEY_COLUMNS];
    uint32 m_numMatches = 0;
    KEY_OPER m_ixOpers = KEY_OPER::READ_INVALID;
    bool m_exact = false;

    double m_cost = 0;

    void Init()
    {
        for (uint j = 0; j < NVM_MAX_KEY_COLUMNS; j++) {
            m_colMatch[j] = nullptr;
            m_parentColMatch[j] = nullptr;
            m_opers[j] = KEY_OPER::READ_INVALID;
            m_params[j] = -1;
            m_col[j] = -1;
        }
        m_ixOpers = KEY_OPER::READ_INVALID;
        m_numMatches = 0;
        m_exact = false;
    }

    inline bool IsUsable() const
    {
        return (m_colMatch[0] != nullptr);
    }

    inline bool IsFullMatch() const
    {
        return (m_numMatches == m_ix->GetColCount());
    }

    inline int32_t GetNumMatchedCols() const
    {
        return m_numMatches;
    }

    bool SetIndexColumn(NVMFdwState *state, uint32 colNum, KEY_OPER op, Expr *expr, Expr *parent);
};

class NvmMatchIndexArr {
public:
    NvmMatchIndexArr() noexcept
    {
        for (uint i = 0; i < NVM_MAX_KEY_COLUMNS; i++) {
            m_idx[i] = nullptr;
        }
    }

    ~NvmMatchIndexArr() noexcept
    {
        Clear(true);
    }

    void Clear(bool release = false)
    {
        for (uint i = 0; i < NVM_MAX_KEY_COLUMNS; i++) {
            if (m_idx[i]) {
                if (release) {
                    pfree(m_idx[i]);
                }
                m_idx[i] = nullptr;
            }
        }
    }
    NvmMatchIndex *m_idx[NVM_MAX_KEY_COLUMNS];
};

}  // namespace NVMDB
#endif
