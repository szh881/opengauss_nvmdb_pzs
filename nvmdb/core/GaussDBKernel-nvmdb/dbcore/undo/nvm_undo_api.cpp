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
 * nvm_undo_api.cpp
 *
 * IDENTIFICATION
 *   src/gausskernel/storage/nvmdb/core/GaussDBKernel-nvmdb/dbcore/undo/nvm_undo_api.cpp
 * -------------------------------------------------------------------------
 */
#include "nvm_undo_api.h"
#include "nvm_undo_segment.h"

namespace NVMDB {

/*
 * Undo 单独占一个tablespace。第一个数据页面里存所有segment的位置。
 */
void UndoCreate(const char *dir)
{
    UndoSegmentCreate(dir);
}

void UndoBootStrap(const char *dir)
{
    UndoSegmentMount(dir);
}

void UndoExitProcess()
{
    UndoSegmentUnmount();
}

}