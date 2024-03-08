/*
 * Copyright (c) 2023 Huawei Technologies Co.,Ltd.
 *
 * openGauss is licensed under Mulan PSL v2.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *
 * http://license.coscl.org.cn/MulanPSL2
 *
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 * -------------------------------------------------------------------------
 *
 * page_dlist.h
 *
 * IDENTIFICATION
 *   src/gausskernel/storage/nvmdb/core/GaussDBKernel-nvmdb/include/page_dlist.h
 * -------------------------------------------------------------------------
 */
#ifndef NVMDB_PAGE_DLIST_H
#define NVMDB_PAGE_DLIST_H

#include "nvm_types.h"

namespace NVMDB {

/*
 * 抽象出一个page double list 的概念。比较多场景下，需要把page连起来，比如同一个 segment 中的page；属于同一个
 * TransactionSlot 的 undo page。注意这个 list 存的是页号，为了节约空间。
 */

typedef struct PageDListNode {
    uint32 prev;
    uint32 next;
} PageDListNode;

class TableSpace;

/* 链表的头就在一个page中，这个page也是属于链表的，那么初始化的时候链表的prev/next 都指向自己 */
void page_dlist_init_head(TableSpace *space, int offset, uint32 node);

/* 是否是链表头，判断依据是长度是否为1 */
bool page_dlist_is_head(TableSpace *space, int offset, uint32 node);

/* 把 node 插到链表的尾部 */
void page_dlist_push_tail(TableSpace *space, int offset, uint32 head, uint32 node);

/* pop 链表的最后一个node */
uint32 page_dlist_pop_tail(TableSpace *space, int offset, uint32 head);

/* 获取下一个节点的页号 */
uint32 page_dlist_next(TableSpace *space, int offset, uint32 node);

/* 删除当前节点 */
void page_dlist_delete(TableSpace *space, int offset, uint32 node);

/* 把整个链表 src 加入到 dst 中 */
void page_dlist_link(TableSpace *space, int offset, uint32 src, uint32 dst);

}  // namespace NVMDB

#endif  // NVMDB_PAGE_DLIST_H