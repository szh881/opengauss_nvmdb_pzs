/* -------------------------------------------------------------------------
 *
 * pactree.h
 *    pactree class definition
 *
 * Portions Copyright (c) 2023 Huawei Technologies Co.,Ltd.
 * Portions Copyright (c) 2019-2021 Virginia Tech
 *
 * IDENTIFICATION
 *	  src/gausskernel/storage/nvmdb/core/GaussDBKernel-nvmdb/third_parties/pactree/include/pactree.h
 *
 * -------------------------------------------------------------------------
 */
#ifndef pactreeAPI_H
#define pactreeAPI_H
#include "pactreeImpl.h"
#include "common.h"

namespace NVMDB {

class pactree {
public:
    pactree(const char *pmem_dir)
    {
        pt = InitPT(pmem_dir);
    }
    ~pactree()
    {
        pt->~pactreeImpl();
        pt = NULL;
        PMem::UnmountPMEMPool();
        LNodeReport();
    }
    bool Insert(Key_t &key, Val_t val)
    {
        return pt->Insert(key, val);
    }
    Val_t lookup(Key_t &key, bool *found)
    {
        return pt->Lookup(key, found);
    }
    void scan(Key_t &startKey, Key_t &endKey, int max_range, LookupSnapshot snapshot, bool reverse,
              std::vector<std::pair<Key_t, Val_t>> &result)
    {
        pt->Scan(startKey, endKey, max_range, snapshot, reverse, result);
    }

    void registerThread(int grpId = 0)
    {
        pt->RegisterThread(grpId);
    }
    void unregisterThread()
    {
        pt->UnregisterThread();
    }
private:
    pactreeImpl *pt;
};

}  // namespace NVMDB

#endif
