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
 * nvm_dbcore.cpp
 *
 * IDENTIFICATION
 *   src/gausskernel/storage/nvmdb/core/GaussDBKernel-nvmdb/dbcore/nvm_dbcore.cpp
 * -------------------------------------------------------------------------
 */
#include "nvm_undo_api.h"
#include "nvm_heap_space.h"
#include "nvmdb_thread.h"
#include "index/nvm_index.h"
#include "nvm_dbcore.h"

namespace NVMDB {

void InitDB(const char *dir)
{
    ParseDirectoryConfig(dir, true);
    InitGlobalVariables();
    UndoCreate(dir);
    HeapCreate(dir);
    IndexBootstrap(dir);
}

void BootStrap(const char *dir)
{
    ParseDirectoryConfig(dir);
    InitGlobalVariables();
    HeapBootStrap(dir);
    IndexBootstrap(dir);
    UndoBootStrap(dir);
}

void ExitDBProcess()
{
    IndexExitProcess();
    HeapExitProcess();
    UndoExitProcess();
    DestroyGlobalVariables();
}

}  // namespace NVMDB