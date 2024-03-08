/* -------------------------------------------------------------------------
 *
 * pmem.h
 *    pmem class definition
 *
 * Portions Copyright (c) 2023 Huawei Technologies Co.,Ltd.
 * Portions Copyright (c) 2019-2021 Virginia Tech
 *
 * IDENTIFICATION
 *	  src/gausskernel/storage/nvmdb/core/GaussDBKernel-nvmdb/third_parties/pactree/include/pmem.h
 *
 * -------------------------------------------------------------------------
 */
#ifndef PACTREE_PMEM_H
#define PACTREE_PMEM_H

#include <libpmemobj.h>
#include <iostream>
#include <unistd.h>
#include <mutex>
#include <assert.h>

#include "Oplog.h"
#include "arch.h"
#include "nvm_cfg.h"

#define MASK 0x8000FFFFFFFFFFFF
#define MASK_DIRTY 0xDFFFFFFFFFFFFFFF  // DIRTY_BIT
#define MASK_POOL 0x7FFFFFFFFFFFFFFF

typedef struct root_obj {
    PMEMoid ptr[2];
} root_obj;

class PMem {
public:
    static const size_t DEFAULT_POOL_SIZE = 4 * 1024 * 1024 * 1024UL;
    static_assert(NVMDB::NVMDB_NUM_LOGS_PER_THREAD * NVMDB::NVMDB_MAX_THREAD_NUM * sizeof(OpStruct) <=
                  PMem::DEFAULT_POOL_SIZE / 2);
    static void CreatePMemPool(const char *dirname, int *is_create, root_obj **root, root_obj **sl_root);
    static int GetPoolNum();
    static void UnmountPMEMPool();
    static void *getBaseOf(int poolId);

    static void alloc(size_t size, void **p, PMEMoid *oid);
    static void free(void *pptr);
    static void freeVaddr(void *vaddr);

    static void *getOpLog(int i);
};

static inline void flushToNVM(char *data, size_t sz)
{}

#endif
