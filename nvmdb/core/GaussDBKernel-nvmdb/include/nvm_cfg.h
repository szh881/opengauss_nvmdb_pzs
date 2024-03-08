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
 *   src/gausskernel/storage/nvmdb/core/GaussDBKernel-nvmdb/include/nvm_cfg.h
 * -------------------------------------------------------------------------
 */
#ifndef NVMDB_CFG_H
#define NVMDB_CFG_H

#include "nvm_types.h"

namespace NVMDB {

static constexpr int NVMDB_MAX_GROUP = 4;

// Always greater than the number of concurrent connections
static constexpr uint32 NVMDB_MAX_THREAD_NUM = 1024;

// for undo
static constexpr int NVMDB_UNDO_SEGMENT_NUM = 2048;
static_assert(NVMDB_UNDO_SEGMENT_NUM >= NVMDB_MAX_THREAD_NUM);

// for pactree oplog
static constexpr int NVMDB_NUM_LOGS_PER_THREAD = 512;
static constexpr int NVMDB_OPLOG_WORKER_THREAD_PER_GROUP = 1;
static constexpr int NVMDB_OPLOG_QUEUE_MAX_CAPACITY = 10000;

}  // namespace NVMDB

#endif
