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
 * nvm_page_dlist.cpp
 *
 * IDENTIFICATION
 *   src/gausskernel/storage/nvmdb/core/GaussDBKernel-nvmdb/dbcore/nvm_page_dlist.cpp
 * -------------------------------------------------------------------------
 */
#include "nvm_types.h"
#include "nvm_table_space.h"
#include "nvm_page_dlist.h"

namespace NVMDB {

static inline PageDListNode *get_dlist_node(TableSpace *space, int offset, uint32 node)
{
    return (PageDListNode *)(space->RelpointOfPageno(node) + offset);
}

void page_dlist_init_head(TableSpace *space, int offset, uint32 node)
{
    auto dlist_node = get_dlist_node(space, offset, node);
    dlist_node->next = dlist_node->prev = node;
}

bool page_dlist_is_head(TableSpace *space, int offset, uint32 node)
{
    auto dlist_node = get_dlist_node(space, offset, node);
    if (dlist_node->prev == node) {
        Assert(dlist_node->next == node);
        return true;
    }
    return false;
}

/* 即使当前链表只有head一个元素也成立 */
void page_dlist_push_tail(TableSpace *space, int offset, uint32 head, uint32 node)
{
    auto head_node = get_dlist_node(space, offset, head);
    auto tail_node = get_dlist_node(space, offset, head_node->prev);
    auto new_node = get_dlist_node(space, offset, node);

    new_node->prev = head_node->prev;
    new_node->next = head;
    tail_node->next = node;
    head_node->prev = node;
}

uint32 page_dlist_pop_tail(TableSpace *space, int offset, uint32 head)
{
    auto head_node = get_dlist_node(space, offset, head);
    auto tail_node = get_dlist_node(space, offset, head_node->prev);
    auto tail_prev = get_dlist_node(space, offset, tail_node->prev);
    uint32 res = head_node->prev;

    head_node->prev = tail_node->prev;
    tail_prev->next = head;

    return res;
}

uint32 page_dlist_next(TableSpace *space, int offset, uint32 node)
{
    auto curr = get_dlist_node(space, offset, node);
    return curr->next;
}

/* 删除当前节点 */
void page_dlist_delete(TableSpace *space, int offset, uint32 node)
{
    auto curr_node = get_dlist_node(space, offset, node);
    auto prev_node = get_dlist_node(space, offset, curr_node->prev);
    auto next_node = get_dlist_node(space, offset, curr_node->next);

    prev_node->next = curr_node->next;
    next_node->prev = curr_node->prev;

    curr_node->prev = curr_node->next = node;
}

/* 把整个链表 src 加入到 dst 中 */
void page_dlist_link(TableSpace *space, int offset, uint32 src, uint32 dst)
{
    /*
     * 两个链表的合并，这里采用的方式是被合并的链表，直接放到目标链表的最前面。
     * 所以 dst->next = src，而src的tail node指向的是目标链表原来的第二个。
     */
    auto src_node = get_dlist_node(space, offset, src);
    auto src_tail_node = get_dlist_node(space, offset, src_node->prev);
    auto dst_node = get_dlist_node(space, offset, dst);
    auto dst_next_node = get_dlist_node(space, offset, dst_node->next);

    dst_next_node->prev = src_node->prev;
    src_tail_node->next = dst_node->next;
    dst_node->next = src;
    src_node->prev = dst;
}

}  // namespace NVMDB