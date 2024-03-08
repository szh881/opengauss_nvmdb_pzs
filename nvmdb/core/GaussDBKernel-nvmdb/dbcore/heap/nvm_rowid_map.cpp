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
 * nvm_rowid_map.cpp
 *
 * IDENTIFICATION
 *   src/gausskernel/storage/nvmdb/core/GaussDBKernel-nvmdb/dbcore/heap/nvm_rowid_map.cpp
 * -------------------------------------------------------------------------
 */
#include <unordered_map>
#include <mutex>

#include "nvm_heap_space.h"
#include "nvm_rowid_map.h"

namespace NVMDB {

/*
 * 这里的难点在于 extend 的时候，segments的指针会指向新的地址；而并发的读可能会读到旧的地址
 * 所以需要用一个 extend flag 来标记。读操作，在读 segments 前后会检查flag 是否有变化。
 * 如果有变化，需要重试一次。
 *
 * 正确性： 一个 segment 一旦创建，其地址不会改变，只要找到它起始地址就可以了。
 *        如果 reader 在读 segments 数组前后，flag没有变化，说明它读的segment值，是正确的。
 *        即使读操作结束之后，再扩展，也不影响该值。
 */
RowIdMapEntry *RowIdMap::GetSegment(int segId)
{
    if (segId >= m_segmentCapacity.load()) {
        Extend(segId);
    }
    while (true) {
        uint32 flag = GetExtendVersion();
        auto segment = reinterpret_cast<RowIdMapEntry *>(m_segments.load()[segId]);
        uint32 newFlag = GetExtendVersion();
        if (segment == nullptr) {
            Extend(segId);
        } else if (newFlag == flag) {
            return segment;
        }
    }
}

void RowIdMap::Extend(int segId)
{
    std::lock_guard<std::mutex> lockGuard(mtx);
    if (segId >= m_segmentCapacity.load()) {
        int newCap = m_segmentCapacity.load() * EXTEND_FACTOR;
        while (newCap <= segId) {
            newCap *= EXTEND_FACTOR;
        }
        auto newSegments = new RowIdMapEntry *[newCap];
        size_t memSize = newCap * sizeof(RowIdMapEntry *);
        int ret = memset_s(newSegments, memSize, 0, memSize);
        SecureRetCheck(ret);
        ret = memcpy_s(newSegments, memSize, m_segments, m_segmentCapacity.load() * sizeof(RowIdMapEntry *));
        SecureRetCheck(ret);

        SetExtendFlag();
        delete[] m_segments.load();
        m_segments.store(newSegments);
        ResetExtendFlag();
        m_segmentCapacity.store(newCap);
    }

    if (m_segments[segId] == nullptr) {
        RowIdMapEntry *segment = new RowIdMapEntry[segment_len];
        size_t memSize = sizeof(RowIdMapEntry) * segment_len;
        int ret = memset_s(segment, memSize, 0, memSize);
        SecureRetCheck(ret);
        m_segments[segId] = segment;
    }
}

RowIdMapEntry *RowIdMap::GetEntry(RowId rowId, bool isRead)
{
    int segId = rowId / segment_len;
    RowIdMapEntry *segment = GetSegment(segId);
    RowIdMapEntry *entry = &segment[rowId % segment_len];

    if (!entry->IsValid()) {
        char *nvmAddr = m_vecstore->VersionPoint(rowId);
        /* not valid row on nvm. */
        if (nvmAddr == nullptr) {
            Assert(isRead);
            return nullptr;
        }
        entry->Lock();
        if (!entry->IsValid()) {
            entry->m_nvmAddr = nvmAddr;
            entry->m_dramCache = nullptr;
            entry->m_flag2 = 0;
            std::atomic_thread_fence(std::memory_order_release);
            entry->SetValid();
        }
        entry->Unlock();
    }
    std::atomic_thread_fence(std::memory_order_acquire);
    return entry;
}

static std::unordered_map<uint32, RowIdMap *> g_globalRowidMaps;
static std::mutex g_grimMtx;
thread_local std::unordered_map<uint32, RowIdMap *> g_localRowidMaps;

static RowIdMap *GetRowIdMapFromGlobalCache(uint32 seghead, uint32 rowLen)
{
    std::lock_guard<std::mutex> lockGuard(g_grimMtx);
    if (g_globalRowidMaps.find(seghead) == g_globalRowidMaps.end()) {
        g_globalRowidMaps[seghead] = new RowIdMap(g_heapSpace, seghead, rowLen);
    }
    return g_globalRowidMaps[seghead];
}

RowIdMap *GetRowIdMap(uint32 seghead, uint32 rowLen)
{
    if (g_localRowidMaps.find(seghead) == g_localRowidMaps.end()) {
        g_localRowidMaps[seghead] = GetRowIdMapFromGlobalCache(seghead, rowLen);
    }
    RowIdMap *result = g_localRowidMaps[seghead];
    Assert(result->GetRowLen() == rowLen);
    return result;
}

void InitGlobalRowIdMapCache()
{
    g_globalRowidMaps.clear();
}

void InitLocalRowIdMapCache()
{
    g_localRowidMaps.clear();
}

void DestroyGlobalRowIdMapCache()
{
    for (auto entry : g_globalRowidMaps) {
        delete entry.second;
    }
    g_globalRowidMaps.clear();
}

void DestroyLocalRowIdMapCache()
{
    g_localRowidMaps.clear();
}

}  // namespace NVMDB