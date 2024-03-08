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
 * nvm_rowid_map.h
 *
 * IDENTIFICATION
 *   src/gausskernel/storage/nvmdb/core/GaussDBKernel-nvmdb/include/heap/nvm_rowid_map.h
 * -------------------------------------------------------------------------
 */
#ifndef NVMDB_ROWID_MAP_H
#define NVMDB_ROWID_MAP_H

#include <cstring>
#include <vector>
#include <mutex>

#include "nvm_vecstore.h"
#include "nvm_table_space.h"
#include "nvm_tuple.h"
#include "securec.h"

namespace NVMDB {

/* flag1 */
#define ROWID_LOCKED 0x01000000
#define ROWID_VALID 0x02000000

constexpr int SEGMENT_CAP = 16;

struct RowIdMapEntry {
    char *m_nvmAddr;
    char *m_dramCache;
    volatile uint32 m_flag1;
    volatile uint32 m_flag2;

    void Lock()
    {
        do {
            uint32 old_flag = m_flag1;
            if (old_flag & ROWID_LOCKED) {
                continue;
            }
            uint32 new_flag = old_flag | ROWID_LOCKED;
            if (__sync_bool_compare_and_swap(&m_flag1, old_flag, new_flag)) {
                return;
            }
        } while (true);
    }

    void Unlock()
    {
        uint32 old_flag = m_flag1;
        Assert(old_flag & ROWID_LOCKED);
        uint32 new_flag = old_flag & ~ROWID_LOCKED;
        std::atomic_thread_fence(std::memory_order_acq_rel);
        m_flag1 = new_flag;
    }

    bool IsValid() const
    {
        return m_flag1 & ROWID_VALID;
    }

    void SetValid()
    {
        m_flag1 |= ROWID_VALID;
    }

    void sync_dram_cache(int tuple_size)
    {
        Assert(tuple_size <= MAX_TUPLE_LEN);
        if (m_dramCache == nullptr) {
            m_dramCache = new char[tuple_size];
        }
        errno_t ret = memcpy_s(m_dramCache, tuple_size, m_nvmAddr, tuple_size);
        SecureRetCheck(ret);
    }

    void sync_dram_cache_deleted()
    {
        if (m_dramCache != nullptr) {
            errno_t ret = memcpy_s(m_dramCache, NVMTupleHeadSize, m_nvmAddr, NVMTupleHeadSize);
            SecureRetCheck(ret);
        }
    }

    char *read_dram_cache(int tuple_size)
    {
        if (m_dramCache == nullptr) {
            Assert(tuple_size <= MAX_TUPLE_LEN);
            m_dramCache = new char[tuple_size];
            errno_t ret = memcpy_s(m_dramCache, tuple_size, m_nvmAddr, tuple_size);
            SecureRetCheck(ret);
        }
        return m_dramCache;
    }
};

class RowIdMap {
    static constexpr int EXTEND_FACTOR = 2;
    std::atomic<uint32> extend_flag;
    std::atomic<RowIdMapEntry **> m_segments{};
    std::atomic<int> segment_number{};
    std::atomic<int> m_segmentCapacity{};

    VecStore *m_vecstore;

    static const int segment_len = 128 * 1024;
    uint32 row_len;
    std::mutex mtx;

    void SetExtendFlag()
    {
        extend_flag.fetch_add(1);
    }

    void ResetExtendFlag()
    {
        extend_flag.fetch_add(1);
    }

    uint32 GetExtendVersion()
    {
        uint32 res = extend_flag.load();
        while (res & 1) {
            res = extend_flag.load();
        }
        return res;
    }

    RowIdMapEntry *GetSegment(int seg_id);
    void Extend(int seg_id);

public:
    RowIdMap(TableSpace *space, uint32 seghead, uint32 _row_len) : row_len(_row_len), extend_flag(0)
    {
        m_segmentCapacity = SEGMENT_CAP;

        m_segments.store(new RowIdMapEntry *[m_segmentCapacity]());

        m_vecstore = new VecStore(space, seghead, _row_len);
        segment_number = 0;
    }

    RowId InsertVersion()
    {
        return m_vecstore->InsertVersion();
    }

    uint32 GetRowLen() const
    {
        return row_len;
    }

    RowId GetUpperRowId()
    {
        return m_vecstore->GetUpperRowId();
    }

    RowIdMapEntry *GetEntry(RowId rowId, bool is_read = false);
};

RowIdMap *GetRowIdMap(uint32 seghead, uint32 row_len);

void InitGlobalRowIdMapCache();
void InitLocalRowIdMapCache();
void DestroyGlobalRowIdMapCache();
void DestroyLocalRowIdMapCache();

}  // namespace NVMDB

#endif  // NVMDB_ROWID_MAP_H
