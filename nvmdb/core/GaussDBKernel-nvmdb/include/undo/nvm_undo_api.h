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
 * nvm_undo_api.h
 *
 * IDENTIFICATION
 *   src/gausskernel/storage/nvmdb/core/GaussDBKernel-nvmdb/include/undo/nvm_undo_api.h
 * -------------------------------------------------------------------------
 */
#ifndef NVMDB_UNDOAPI_H
#define NVMDB_UNDOAPI_H

#include "nvm_undo_internal.h"
#include "nvm_undo_segment.h"
#include "nvm_undo_context.h"

namespace NVMDB {

/* initdb 时调用 */
void UndoCreate(const char *dir);

/* 数据库启动时调用，初始化基本信息，启动清理线程。 */
void UndoBootStrap(const char *dir);

/* 事务启动时调用，绑定事务的 undo context；事务执行过程中通过UndoLocalContext插入undo日志 */
UndoTrxContext *AllocUndoContext();

/* 根据 undo ptr 获得具体的 undo record */
extern UndoRecord *CopyUndoRecord(UndoRecPtr ptr, char* undo_record_cache);

/* 事务结束时（完成提交或者回滚）,把对应undo页面标记位待回收的状态。 */
void ReleaseTrxUndoContext(UndoTrxContext* undo_trx);

/* 正常退出时清理undo信息 */
void UndoExitProcess();

}

#endif // NVMDB_UNDOAPI_H