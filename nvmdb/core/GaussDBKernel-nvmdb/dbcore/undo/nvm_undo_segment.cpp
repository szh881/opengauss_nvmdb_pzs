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
 * nvm_undo_segment.cpp
 *
 * IDENTIFICATION
 *   src/gausskernel/storage/nvmdb/core/GaussDBKernel-nvmdb/dbcore/undo/nvm_undo_segment.cpp
 * -------------------------------------------------------------------------
 */
#include <mutex>
#include <cstring>
#include <thread>
#include <unistd.h>

#include "nvm_transaction.h"
#include "nvm_undo_rollback.h"
#include "nvmdb_thread.h"
#include "nvm_undo_segment.h"
#include "nvm_cfg.h"

namespace NVMDB {

static UndoSegment *g_undo_segment_padding[NVMDB_UNDO_SEGMENT_NUM + 16];
static UndoSegment **g_undo_segments = &g_undo_segment_padding[16];
static bool g_undo_segment_allocated[NVMDB_UNDO_SEGMENT_NUM];

constexpr int SLOT_OFFSET = 2;

static const char *g_undoFilename = "undo";

thread_local UndoSegment *t_undo_segment = nullptr;
thread_local uint64 my_clock = 0;
static uint64 clock_sweep = 0;
std::mutex g_undoSegmentLock;

std::thread g_undoRecycle;
static bool g_doRecycle = true;

void UndoSegmentInitHead(UndoSegmentHead *head)
{
    errno_t ret = memset_s(head, sizeof(UndoSegmentHead), 0, sizeof(UndoSegmentHead));
    if (ret != EOK) {
        return;
    }
    head->free_begin = sizeof(UndoSegmentHead);
    head->recycled_begin = sizeof(UndoSegmentHead);
    head->next_free_slot = 0;
    head->next_recycle_slot = 0;
    head->min_slot_id = 0;
}

static std::string generate_undo_filename(uint32 segment_id)
{
    return std::string(g_undoFilename) + std::to_string(segment_id);
}

UndoSegment::UndoSegment(const char *dir, uint32 segment_id)
    : segid(segment_id),
      filename(generate_undo_filename(segment_id)),
      LogicFile(dir, generate_undo_filename(segment_id).c_str(), UNDO_SLICE_SIZE, UNDO_MAX_SLICE_NUM)
{}

void UndoSegment::RollBack(TransactionSlot *trx_slot, char *undo_record_cache)
{
    if (UndoRecPtrIsInValid(trx_slot->start)) {
        Assert(UndoRecPtrIsInValid(trx_slot->end));
        return;
    }

    UndoRecPtr undo_ptr = trx_slot->end;
    while (!UndoRecPtrIsInValid(undo_ptr)) {
        Assert(undo_ptr >= trx_slot->start && undo_ptr <= trx_slot->end);
        UndoRecord *undo_record = CopyUndoRecord(undo_ptr, undo_record_cache);
        UndoRecordRollBack(undo_record);
        undo_ptr = undo_record->m_pre;
    }
}

void UndoSegment::Recovery(uint64 &max_undo_csn)
{
    if (trxslot_is_empty()) {
        if (max_undo_csn < seghead->min_snapshot) {
            max_undo_csn = seghead->min_snapshot;
        }
        return;
    }
    Assert(seghead->next_free_slot >= 1);
    /* the last 2 allocated trx slot.  */
    uint64 slot_begin = 0;
    uint64 slot_end = seghead->next_free_slot - 1;
    if (slot_end != 0) {
        slot_begin = slot_end - 1;
    }
    TransactionSlot *trx_slot = nullptr;
    uint64 undo_csn = 0;
    for (uint64 i = slot_begin; i <= slot_end; i++) {
        trx_slot = &seghead->trxslots[i % UNDO_TRX_SLOTS];
        uint32 tx_status = trx_slot->status;
        undo_csn = trx_slot->CSN;
        if (tx_status == TRX_COMMITTED && undo_csn > max_undo_csn) {
            /* the used max csn */
            max_undo_csn = undo_csn;
        } else if (tx_status == TRX_IN_PROGRESS) {
            /* do roll back */
        } else {
            /* already rollback  */
        }
    }

    if (seghead->recovery_start == 0) {
        /* safe to update recovery restart point; Otherwise means that last crash happens during recovery */
        seghead->recovery_start = slot_begin + 1;
    }
    seghead->recovery_end = slot_end;
}

void UndoSegment::BGRecovery()
{
    for (uint64 i = seghead->recovery_start; i <= seghead->recovery_end; i++) {
        auto trx_slot = &seghead->trxslots[i % UNDO_TRX_SLOTS];
        uint32 tx_status = trx_slot->status;

        if (tx_status == TRX_IN_PROGRESS) {
            char *undo_record_cache = new char[MAX_UNDO_RECORD_CACHE_SIZE];
            RollBack(trx_slot, undo_record_cache);
            trx_slot->status = TRX_ROLLBACKED;
            delete[] undo_record_cache;
        }
    }
    seghead->recovery_start = 0;
}

/* copy to read trx slot content AS undo recycle runs background. */
bool UndoSegment::GetTransactionSlot(uint64 slot_id, TransactionSlot *trx_slot)
{
    Assert(trx_slot != nullptr);
    errno_t ret = memcpy_s(trx_slot, sizeof(TransactionSlot), &seghead->trxslots[slot_id % UNDO_TRX_SLOTS],
                           sizeof(TransactionSlot));
    SecureRetCheck(ret);

    if (slot_id < seghead->min_slot_id.load(std::memory_order_acquire)) {
        return false;
    }
    /* never load a recycled slot. */
    Assert(trx_slot->status != TRX_EMPTY);
    return true;
}

bool UndoSegment::GetTransactionInfo(uint64 slot_id, TransactionInfo *trx_info)
{
    TransactionSlot slot;
    /* already recycled. */
    if (!GetTransactionSlot(slot_id, &slot)) {
        return false;
    }

    trx_info->CSN = slot.CSN;
    trx_info->status = (TransactionSlotStatus)slot.status;

    while (trx_info->status == TRX_COMMITTED && !TrxInfoIsCsn(trx_info->CSN)) {
        /* already recycled. */
        if (!GetTransactionSlot(slot_id, &slot)) {
            return false;
        }
        /* 事务正在提交，但是CSN还没来得及设 */
        trx_info->CSN = slot.CSN;
    }
    return true;
}

/* return pointer to trx slot when allocate. */
TransactionSlot *UndoSegment::GetTransactionSlot(uint64 slot_id)
{
    Assert(slot_id == seghead->next_free_slot.load(std::memory_order_relaxed));
    return &seghead->trxslots[slot_id % UNDO_TRX_SLOTS];
}

bool UndoSegment::trxslot_is_available(TransactionSlot *trx_slot)
{
    return (trx_slot->status == TRX_EMPTY || trx_slot->status == TRX_ROLLBACKED);
}

bool UndoSegment::trxslot_is_recyclable(TransactionSlot *trx_slot, uint64 min_snapshot)
{
    TransactionInfo info = GetTransactionInfoSafe(trx_slot);
    return (info.status == TRX_COMMITTED && info.CSN < min_snapshot);
}

bool UndoSegment::trxslot_is_empty()
{
    return seghead->next_free_slot.load(std::memory_order_relaxed) ==
           seghead->next_recycle_slot.load(std::memory_order_relaxed);
}

bool UndoSegment::trxslot_is_full()
{
    return seghead->next_free_slot.load(std::memory_order_relaxed) ==
           seghead->next_recycle_slot.load(std::memory_order_relaxed) + UNDO_TRX_SLOTS;
}

/* take undo record heap into consideration if you loop use it. */
bool UndoSegment::SegmentFull()
{
    return trxslot_is_full();
}

bool UndoSegment::SegmentEmpty()
{
    return trxslot_is_empty();
}

uint64 UndoSegment::GetNextTrxSlot()
{
    Assert(!trxslot_is_full());

    uint64 trx_slot_id = seghead->next_free_slot.load(std::memory_order_relaxed);
    Assert(trx_slot_id <= seghead->next_recycle_slot.load(std::memory_order_relaxed) + UNDO_TRX_SLOTS);
    return trx_slot_id;
}

UndoRecPtr UndoSegment::InsertUndoRecord(UndoRecord *record)
{
    int undo_size = UndoRecTotalSize(record->m_payload);
    Assert(undo_size <= MAX_UNDO_RECORD_CACHE_SIZE);
    write_to_slice(seghead->free_begin, (char *)record, undo_size);
    UndoRecPtr ptr = AssembleUndoRecPtr(segid, seghead->free_begin);

    seghead->free_begin += undo_size;
    return ptr;
}

void UndoSegment::write_to_slice(uint64 vptr, char *src, int len)
{
    int slice_remain = SLICE_LEN - (vptr % SLICE_LEN);
    uint32 pageno = vptr / NVM_BLCKSZ;
    uint32 offset = vptr % NVM_BLCKSZ;
    if (slice_remain >= len) {
        extend(pageno);
        errno_t ret = memcpy_s(RelpointOfPageno(pageno) + offset, len, src, len);
        if (ret != EOK) {
            return;
        }
    } else {
        extend(pageno);
        extend(pageno + 1);
        errno_t ret = memcpy_s(RelpointOfPageno(pageno) + offset, slice_remain, src, slice_remain);
        if (ret != EOK) {
            return;
        }
        Assert((pageno + 1) % SLICE_BLOCKS == 0);
        ret = memcpy_s(RelpointOfPageno(pageno + 1), len - slice_remain, src + slice_remain, len - slice_remain);
        if (ret != EOK) {
            return;
        }
    }
}

void UndoSegment::copy_from_slice(uint64 vptr, char *dst, int len)
{
    Assert(len < SLICE_LEN);
    int remain_size = SLICE_LEN - (vptr % SLICE_LEN);
    uint32 pageno = vptr / NVM_BLCKSZ;
    uint32 offset = vptr % NVM_BLCKSZ;

    if (remain_size >= len) {
        extend(pageno);
        errno_t ret = memcpy_s(dst, len, RelpointOfPageno(pageno) + offset, len);
        if (ret != EOK) {
            return;
        }
    } else {
        extend(pageno + 1);
        errno_t ret = memcpy_s(dst, remain_size, RelpointOfPageno(pageno) + offset, remain_size);
        if (ret != EOK) {
            return;
        }
        Assert((pageno + 1) % SLICE_BLOCKS == 0);
        ret = memcpy_s(dst + remain_size, len - remain_size, RelpointOfPageno(pageno + 1), len - remain_size);
        if (ret != EOK) {
            return;
        }
    }
}

UndoRecord *UndoSegment::CopyUndoRecord(UndoRecPtr undo, char *undo_record_cache)
{
    Assert(UndoRecPtrGetSegment(undo) == segid);
    Assert(undo_record_cache != nullptr);

    uint64 ptr = UndoRecPtrGetOffset(undo);

    UndoRecord undo_head;
    copy_from_slice(ptr, (char *)&undo_head, UndoRecHeadSize);

    copy_from_slice(ptr, undo_record_cache, UndoRecTotalSize(undo_head.m_payload));
    return (UndoRecord *)undo_record_cache;
}

void UndoSegment::RecycleUndoPages(const uint64 &begin_slot, const uint64 &end_slot)
{
    uint32 start_sliceno = seghead->recycled_begin / SLICE_LEN;
    uint32 end_sliceno = 0;
    uint64 recycled_end = 0;
    TransactionSlot *trx_slot = nullptr;

    Assert(begin_slot <= end_slot);
    for (uint64 i = begin_slot; i <= end_slot; i++) {
        trx_slot = &seghead->trxslots[i % UNDO_TRX_SLOTS];
        if (trx_slot->start == 0) {
            Assert(trx_slot->end == 0);
            continue;
        }
        Assert(trx_slot->end != 0);
        recycled_end = UndoRecPtrGetOffset(trx_slot->end);
        end_sliceno = recycled_end / SLICE_LEN;
    }
    /* first slice kept as seg header. */
    if (start_sliceno == 0) {
        start_sliceno = 1;
    }
    if (start_sliceno < end_sliceno) {
        seghead->recycled_begin = recycled_end;
        LogicFile::punch(start_sliceno, end_sliceno);
    }
}

/*
 * Note that this function is invoked by another thread, so deal with shared variables carefully.
 * If you wanna change instruction order, you'd better check GetTransactionSlot.
 */
void UndoSegment::RecycleTransactionSlot(uint64 min_snapshot)
{
    uint64 next_slot = seghead->next_recycle_slot.load(std::memory_order_relaxed);
    uint64 begin_slot = next_slot;
    uint64 max_slot = seghead->next_free_slot.load(std::memory_order_relaxed);
    bool recycled = false;
    while (next_slot < max_slot) {
        TransactionSlot *trx_slot = &seghead->trxslots[next_slot % UNDO_TRX_SLOTS];
        Assert(trx_slot->status != TRX_EMPTY);
        if (trx_slot->status == TRX_ROLLBACKED || trxslot_is_recyclable(trx_slot, min_snapshot)) {
            next_slot++;
            recycled = true;
        } else {
            break;
        }
    }
    if (recycled) {
        /* Undo recovery need min next available csn to boostrap. */
        if (next_slot + SLOT_OFFSET >= max_slot) {
            Assert(seghead->min_snapshot <= min_snapshot);
            seghead->min_snapshot = min_snapshot;
        }
        /* we must update min_slot_id first, so any concurrent check will find the slot is recycled
         * (min_slot_id updated) before the slot is reused (next_recycle_slot updated) */
        seghead->min_slot_id.store(next_slot, std::memory_order_relaxed);

        std::atomic_thread_fence(std::memory_order_seq_cst);

        /* RE-ORDER NOT ALLOWED HERE. */
        next_slot = seghead->min_slot_id;
        RecycleUndoPages(begin_slot, next_slot - 1);

        Assert(next_slot != begin_slot);
        uint64 begin_offset = begin_slot % UNDO_TRX_SLOTS;
        uint64 end_offset = next_slot % UNDO_TRX_SLOTS;
        if (begin_offset < end_offset) {
            errno_t ret =
                memset_s(&seghead->trxslots[begin_offset], (end_offset - begin_offset) * sizeof(TransactionSlot), 0,
                         (end_offset - begin_offset) * sizeof(TransactionSlot));
                SecureRetCheck(ret);
        } else {
            errno_t ret =
                memset_s(&seghead->trxslots[begin_offset], (UNDO_TRX_SLOTS - begin_offset) * sizeof(TransactionSlot), 0,
                         (UNDO_TRX_SLOTS - begin_offset) * sizeof(TransactionSlot));
            SecureRetCheck(ret);
            ret = memset_s(&seghead->trxslots[0], end_offset * sizeof(TransactionSlot), 0,
                           end_offset * sizeof(TransactionSlot));
            SecureRetCheck(ret);
        }

        seghead->next_recycle_slot.store(next_slot, std::memory_order_release);
    }
}

void UndoRecycle()
{
    pthread_setname_np(pthread_self(), "NVM UndoRecycle");
    uint64 minSnapshot = MIN_TRX_CSN;
    uint64 tmpSnapshot = 0;
    while (g_doRecycle) {
        tmpSnapshot = GetMinSnapshot();
        Assert(tmpSnapshot != 0);
        if (tmpSnapshot > minSnapshot) {
            minSnapshot = tmpSnapshot;
        } else {
            continue;
        }
        for (int i = 0; i < NVMDB_UNDO_SEGMENT_NUM; i++) {
            UndoSegment *undoSegment = g_undo_segments[i];
            /* necessary to recycle full undo segment. */
            if (!g_undo_segment_allocated[i] && !undoSegment->SegmentFull()) {
                continue;
            }
            undoSegment->RecycleTransactionSlot(minSnapshot);
        }
    }
}

void UndoSegmentCreate(const char *dir)
{
    for (int i = 0; i < NVMDB_UNDO_SEGMENT_NUM; i++) {
        g_undo_segments[i] = new UndoSegment(g_dirPaths[i % g_dirPathNum].c_str(), i);
        g_undo_segments[i]->Create();
        g_undo_segment_allocated[i] = false;
    }
    g_undoRecycle = std::thread(UndoRecycle);
}

void UndoBGRecovery()
{
    InitThreadLocalVariables();
    for (int i = 0; i < NVMDB_UNDO_SEGMENT_NUM; i++) {
        auto segment = g_undo_segments[i];
        segment->BGRecovery();
    }
    DestroyThreadLocalVariables();
    UndoRecycle();
}

/* must be invoked after undo tablespace is mounted */
void UndoSegmentMount(const char *dir)
{
    uint64 max_undo_csn = MIN_TRX_CSN;
    for (int i = 0; i < NVMDB_UNDO_SEGMENT_NUM; i++) {
        g_undo_segments[i] = new UndoSegment(g_dirPaths[i % g_dirPathNum].c_str(), i);
        g_undo_segments[i]->Mount();
        g_undo_segment_allocated[i] = false;
        g_undo_segments[i]->Recovery(max_undo_csn);
    }

    RecoveryCSN(max_undo_csn);
    // the recycle thread will do the recovery first
    g_undoRecycle = std::thread(UndoBGRecovery);
}

void UndoSegmentUnmount()
{
    g_doRecycle = false;
    g_undoRecycle.join();
    for (int i = 0; i < NVMDB_UNDO_SEGMENT_NUM; i++) {
        Assert(!g_undo_segment_allocated[i]);
        g_undo_segments[i]->UnMount();
        delete g_undo_segments[i];
        g_undo_segments[i] = NULL;
    }
    clock_sweep = 0;
}

UndoSegment *GetUndoSegment(int segid)
{
    return g_undo_segments[segid];
}

void InitLocalUndoSegment()
{
    if (t_undo_segment == NULL) {
        g_undoSegmentLock.lock();
        while (true) {
            clock_sweep++;
            my_clock = clock_sweep % NVMDB_UNDO_SEGMENT_NUM;
            if (g_undo_segment_allocated[my_clock]) {
                continue;
            }
            t_undo_segment = g_undo_segments[my_clock];
            Assert(g_dirPathNum == g_dirPaths.size());
            if (t_undo_segment->MyId() % g_dirPathNum != GetCurrentGroupId() % g_dirPathNum) {
                continue;
            }
            if (t_undo_segment->SegmentFull()) {
                continue;
            }
            g_undo_segment_allocated[my_clock] = true;
            break;
        }
        g_undoSegmentLock.unlock();
    }
}

void DestroyLocalUndoSegment()
{
    if (t_undo_segment != NULL) {
        /* todo: undo_segment can be reused by other threads */
        t_undo_segment = NULL;
        g_undo_segment_allocated[my_clock] = false;
    }
}

UndoSegment *PickSegmentForTrx()
{
    Assert(t_undo_segment != NULL);
    return t_undo_segment;
}

void SwitchUndoSegmentIfFull()
{
    Assert(t_undo_segment != NULL);
    if (!t_undo_segment->SegmentFull()) {
        return;
    }
    DestroyLocalUndoSegment();
    InitLocalUndoSegment();
    Assert(!t_undo_segment->SegmentFull());
}

bool GetTransactionInfo(TransactionSlotPtr trx_ptr, TransactionInfo *trx_info)
{
    uint32 segid = TrxSlotPtrGetSegmentId(trx_ptr);
    Assert(segid < NVMDB_UNDO_SEGMENT_NUM);
    UndoSegment *undo_segment = g_undo_segments[segid];
    uint32 slotid = TrxSlotPtrGetTrxId(trx_ptr);
    return undo_segment->GetTransactionInfo(slotid, trx_info);
}

}  // namespace NVMDB