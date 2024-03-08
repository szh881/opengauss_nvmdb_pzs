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
 * nvm_dbcore.h
 *
 * IDENTIFICATION
 *   src/gausskernel/storage/nvmdb/core/GaussDBKernel-nvmdb/include/nvm_dbcore.h
 * -------------------------------------------------------------------------
 */
#ifndef NVMDB_DBCORE_H
#define NVMDB_DBCORE_H

#include <stddef.h>
#include <string>
#include <vector>

namespace NVMDB {

/* 创建数据库初始环境 */
void InitDB(const char *dir);

/* 数据库启动，进行必要的初始化。 */
void BootStrap(const char *dir);

/* 进程退出时调用，清理内存变量 */
void ExitDBProcess();

}  // namespace NVMDB

#endif
