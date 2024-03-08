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
 * nvm_undo_segment.h
 *
 * IDENTIFICATION
 *   src/gausskernel/storage/nvmdb/core/GaussDBKernel-nvmdb/include/undo/nvm_undo_segment.h
 * -------------------------------------------------------------------------
 */
#ifndef NVMDB_UNDO_SEGMENT_H
#define NVMDB_UNDO_SEGMENT_H

#include <mutex>
#include <atomic>

#include "nvm_undo_internal.h"
#include "nvm_undo_page.h"
#include "nvm_undo_record.h"
#include "nvm_types.h"
#include "nvm_logic_file.h"
#include "nvm_cfg.h"

namespace NVMDB {

static const int UNDO_TRX_SLOTS = CompileValue(512 * 1024, 8 * 1024);
static const size_t UNDO_SLICE_SIZE = CompileValue(64 * 1024 * 1024, 1024 * 1024);
static const size_t UNDO_MAX_SLICE_NUM = 16;
static const size_t UNDO_MAX_OFFSET = 1llu << 40;

/*
 * 前16位， segment id,  后48位，segment 内 trx slot id
 * 但是实际不会有 1<<48 这么多个 trx slot，实际在文件头部存有 UNDO_TRX_SLOTS 个 slot， slot id通过求模映射到对应的位置。
 * segment head 中的 min_slot_id 表示最小的 trx slot id，更小的都被回收掉了。
 * */
typedef uint64 TransactionSlotPtr;

static const uint32 TSP_SLOT_ID_BIT = 48;
static const uint32 TSP_SEGMENT_ID_BIT = 16;
static const uint64 TSP_SLOT_ID_MASK = (1llu << TSP_SLOT_ID_BIT) - 1;
static const uint64 TSP_SEGMENT_ID_MASK = ~TSP_SLOT_ID_MASK;

typedef struct UndoSegmentHead {
    uint64 min_snapshot; /* next available csn to boostrap if all slots recycled. */
    uint64 free_begin;  /* next free space for undo record */
    uint64 recycled_begin; /* next undo record to be recycled */
    uint64 recovery_start;
    uint64 recovery_end;
    std::atomic<uint64> next_free_slot;  /* 下一个可用的 trx slot id; 分配的时候从这里开始 */
    std::atomic<uint64> next_recycle_slot;   /* 下一个需要回收的 slot id;  分配的时候不得超过这个限制，回收的时候会往前推这个下标 */
    std::atomic<uint64> min_slot_id; /* min transaction slot id; any smaller transactions slot id is recycled */
    TransactionSlot trxslots[UNDO_TRX_SLOTS]; /* transaction slots, 2KB */
} UndoSegmentHead;

/* ensure the undo segment head can be located in the first slice */
static_assert(UNDO_SLICE_SIZE >= sizeof(UndoSegmentHead));

void UndoSegmentInitHead(UndoSegmentHead *head);

static_assert(NVMDB_UNDO_SEGMENT_NUM <= (1 << TSP_SEGMENT_ID_BIT));

static inline TransactionSlotPtr GenerateTrxSlotPtr(uint32 segment_id, uint64 slot_id)
{
    Assert(segment_id < NVMDB_UNDO_SEGMENT_NUM);

    return ((uint64)segment_id << TSP_SLOT_ID_BIT) | slot_id;
}

static inline uint32 TrxSlotPtrGetSegmentId(TransactionSlotPtr ptr)
{
    return ptr >> TSP_SLOT_ID_BIT;
}

static inline uint64 TrxSlotPtrGetTrxId(TransactionSlotPtr ptr)
{
    return ptr & TSP_SLOT_ID_MASK;
}

class UndoSegment : public LogicFile {
    uint32 segid;
    UndoSegmentHead *seghead; /* pointer to segment head, note that it's non-volatile */
    std::string filename;
    std::mutex mtx;

    bool trxslot_is_full();
    bool trxslot_is_empty();
    static bool trxslot_is_available(TransactionSlot *trx_slot);
    static bool trxslot_is_recyclable(TransactionSlot *trx_slot, uint64 min_csn);

    void write_to_slice(uint64 vptr, char *src, int len);
    void copy_from_slice(uint64 vptr, char *dst, int len);

    /* Return false means the transaction slot is recycled */
    bool GetTransactionSlot(uint64 slot_id, TransactionSlot* trx_slot);

    void RecycleUndoPages(const uint64& begin_slot, const uint64& end_slot);

public:
    UndoSegment(const char *dir, uint32 segment_id);

    void RollBack(TransactionSlot* trx_slot, char* undo_record_cache);

    void Recovery(uint64& max_undo_csn);

    void BGRecovery();

    void Create() override
    {
        LogicFile::Create();
        seghead = (UndoSegmentHead *)RelpointOfPageno(0);
        UndoSegmentInitHead(seghead);
    }

    void Mount() override
    {
        LogicFile::Mount();
        Assert(SliceNumber() > 0);
        seghead = (UndoSegmentHead *)RelpointOfPageno(0);
    }

    uint32 MyId() const
    {
        return segid;
    }

    bool SegmentFull();
    bool SegmentEmpty();

    uint64 GetNextTrxSlot();
    inline void AdvanceTrxSlot()
    {
        seghead->next_free_slot.fetch_add(1, std::memory_order_relaxed);
    }

    /* Return false means the transaction slot is recycled */
    bool GetTransactionInfo(uint64 slot_id, TransactionInfo *trx_info);

    TransactionSlot *GetTransactionSlot(uint64 slot_id);

    /* Any transaction with csn smaller than min_csn can be recycled */
    void RecycleTransactionSlot(uint64 min_csn);

    UndoRecPtr InsertUndoRecord(UndoRecord *record);
    UndoRecord *CopyUndoRecord(UndoRecPtr undo, char* undo_record_cache);
};

void UndoSegmentCreate(const char *dir);
void UndoSegmentMount(const char *dir);
void UndoSegmentUnmount();
UndoSegment *PickSegmentForTrx();
void SwitchUndoSegmentIfFull();
bool GetTransactionInfo(TransactionSlotPtr trx_ptr, TransactionInfo *trx_info);
UndoSegment *GetUndoSegment(int segid);

void InitLocalUndoSegment();
void DestroyLocalUndoSegment();

}

#endif  // NVMDB_UNDO_SEGMENT_H