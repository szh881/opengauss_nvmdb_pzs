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

#include "nvm_fdw_iter.h"
#include "nvm_index_tuple.h"
#include "nvm_fdw_internal.h"

namespace NVMDB {

bool NvmMatchIndex::SetIndexColumn(NVMFdwState *state, uint32 colNum, KEY_OPER op, Expr *expr, Expr *parent)
{
    bool ret = false;
    uint32 colCount = m_ix->GetColCount();
    const IndexColumnDesc *indexDesc = m_ix->GetIndexDesc();
    uint32 tableColCount = state->mTable->GetColCount();

    for (uint32 i = 0; i < colCount; i++) {
        if (indexDesc[i].m_colId == colNum) {
            if (m_colMatch[i] == nullptr) {
                m_parentColMatch[i] = parent;
                m_colMatch[i] = expr;
                m_opers[i] = op;
                m_col[i] = colNum;
                m_numMatches++;
                if (op == KEY_OPER::READ_KEY_EXACT) {
                    m_exact = true;
                }
                ret = true;
                break;
            } else {
                if (op == KEY_OPER::READ_KEY_EXACT && op != m_opers[i]) {
                    m_parentColMatch[i] = parent;
                    m_colMatch[i] = expr;
                    m_opers[i] = op;
                    m_col[i] = colNum;
                    m_exact = true;
                    ret = true;
                    break;
                }
            }
        }
    }

    return ret;
}

}  // namespace NVMDB
