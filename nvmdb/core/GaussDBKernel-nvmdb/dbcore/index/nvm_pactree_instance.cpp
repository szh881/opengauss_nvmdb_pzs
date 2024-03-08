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
 * nvm_pactree_instance.cpp
 *
 * IDENTIFICATION
 *   src/gausskernel/storage/nvmdb/core/GaussDBKernel-nvmdb/dbcore/index/nvm_pactree_instance.cpp
 * -------------------------------------------------------------------------
 */
#include "pactree.h"

namespace NVMDB {

static pactree *g_pt = nullptr;

void IndexBootstrap(const char *dir)
{
    Assert(g_pt == nullptr);
    g_pt = new pactree(dir);
}

pactree *GetGlobalPACTree()
{
    Assert(g_pt != nullptr);
    return g_pt;
}

void IndexExitProcess()
{
    delete g_pt;
    g_pt = nullptr;
}

void InitLocalIndex(int grpId)
{
    Assert(g_pt != nullptr);
    g_pt->registerThread(grpId);
}

void DestroyLocalIndex()
{
    Assert(g_pt != nullptr);
    g_pt->unregisterThread();
}

}  // namespace NVMDB