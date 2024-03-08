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
 * nvm_index_iter.h
 *
 * IDENTIFICATION
 *   src/gausskernel/storage/nvmdb/core/GaussDBKernel-nvmdb/include/index/nvm_index_iter.h
 * -------------------------------------------------------------------------
 */
#ifndef NVMDB_INDEX_ITER_H
#define NVMDB_INDEX_ITER_H

#include "nvm_types.h"
#include "pactree.h"
#include "codec/nvm_codec.h"

namespace NVMDB {

pactree *GetGlobalPACTree();

class NVMIndexIter {
    std::vector<std::pair<Key_t, Val_t>> result;
    bool valid;
    int cursor;
    int max_size;
    bool reverse;
    Key_t key_end;
    LookupSnapshot snapshot;
    static const int default_range = 6;

    RowId Decode(Key_t *key)
    {
        char *buf = key->getData();
        buf += key->keyLength - 1 - sizeof(uint32);
        Assert(*buf == CODE_ROWID);
        return (RowId)DecodeUint32(buf + 1);
    }

    void search(Key_t &kb, Key_t &ke, int max_range, LookupSnapshot snapshot, bool reverse)
    {
        auto pt = GetGlobalPACTree();
        pt->scan(kb, ke, max_range, snapshot, reverse, result);
        valid = !result.empty();
    }

public:
    /* max_range = 0, means no limit */
    NVMIndexIter(Key_t &kb, Key_t &ke, LookupSnapshot ss, int max_range, bool r)
        : cursor(0), key_end(ke), snapshot(ss), max_size(max_range), reverse(r)
    {
        if (max_range == 0) {
            search(kb, ke, default_range, ss, r);
        } else {
            search(kb, ke, max_range, ss, r);
        }
    }

    void Next()
    {
        cursor++;
        if (cursor >= result.size()) {
            if (max_size == 0) {
                // no size limit, use default_range
                Key_t new_kb = LastKey();
                new_kb.NextKey();
                search(new_kb, key_end, default_range, snapshot, reverse);
                cursor = 0;
            } else {
                // exceed range limit, set invalid
                valid = false;
            }
        }
    }

    bool Valid() const
    {
        return valid;
    }

    RowId Curr()
    {
        Assert(valid);
        return Decode(&result[cursor].first);
    }

    Key_t &LastKey()
    {
        return result.back().first;
    }
};

}  // namespace NVMDB

#endif  // NVMDB_INDEX_ITER_H
