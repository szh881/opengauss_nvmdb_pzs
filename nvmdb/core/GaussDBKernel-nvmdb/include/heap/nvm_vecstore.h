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
 * nvm_vecstore.h
 *
 * IDENTIFICATION
 *   src/gausskernel/storage/nvmdb/core/GaussDBKernel-nvmdb/include/heap/nvm_vecstore.h
 * -------------------------------------------------------------------------
 */
#ifndef NVMDB_VECSTORE_H
#define NVMDB_VECSTORE_H

#include <mutex>

#include "nvm_block.h"
#include "nvm_table_space.h"
#include "nvm_global_bitmap.h"
#include "nvm_rowid_mgr.h"

namespace NVMDB {
/*
 * All tuples in a table are logically in a vector indexed by row id. The vector is implemented as a two-level page
 * table. The segment head of the table is the first level page (root page), storing page number of all second level
 * pages (leaf pages). Leaf pages store tuples.
 *
 * The procedure of allocated an new RowID:
 *     1. Find a unique RowID according to local cache and global bitmap.
 *     2. If corresponding physic page does not exist, allocating a new one.
 *     3. If corresponding physic page exists, and corresponding tuple is used, then return to step 1 and find a new
 *        RowId. This scenario happens after recovery, as global bitmap is reset. In future, if we make FSM info
 *        persistent, this step can be eliminated.
 */
class VecStore {
    RowId TryNextRowid();

    uint32 m_seghead{0};
    uint32 m_tupleLen{0};
    uint32 m_tuplesPerpage{0};
    RowIDMgr *m_rowidMgr{nullptr};

    std::mutex mtx;

    TableSpace *m_tblspc{nullptr};
    GlobalBitMap **m_gbm{nullptr};

public:
    VecStore(TableSpace *_tblspc, uint32 _seghead, uint32 row_len);
    ~VecStore();

    /* 对应的页面是否存在。以及是否有人占，重启之后需要这种方法来确认。如果失败返回NULL */
    char *TryAt(RowId rid);

    RowId InsertVersion();
    char *VersionPoint(RowId row_id);

    /* upper bound RowId in highest allocated range */
    RowId GetUpperRowId();
};

}  // namespace NVMDB

#endif  // NVMDB_VECSTORE_H