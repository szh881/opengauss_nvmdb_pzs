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
 * nvm_logic_file.cpp
 *
 * IDENTIFICATION
 *   src/gausskernel/storage/nvmdb/core/GaussDBKernel-nvmdb/dbcore/table_space/nvm_logic_file.cpp
 * -------------------------------------------------------------------------
 */
#include <libpmem.h>
#include <experimental/filesystem>

#include "nvm_logic_file.h"
#include "nvm_cfg.h"

namespace NVMDB {

std::vector<std::string> g_dirPaths;

uint32 g_dirPathNum;

constexpr mode_t PMEM_MODE = 0666;

void ParseDirectoryConfig(const char *dirNames, bool isInit, uint32 &dirPathNum,
                          std::vector<std::string> &dirPaths)
{
    std::string dirs = dirNames;
    Assert(!dirs.empty());
    std::string dir;
    std::string delimiter = DELIMITER;
    uint32 pos = 0;
    dirPaths.clear();
    Assert(dirPaths.empty());
    while (!dirs.empty()) {
        pos = dirs.find(delimiter);
        dir = dirs.substr(0, pos);
        if (!dir.empty()) {
            dirPaths.push_back(dir);
        }
        dirs.erase(0, pos + delimiter.length());
    }
    dirPathNum = dirPaths.size();
    ALWAYS_CHECK(dirPathNum > 0 && dirPathNum <= NVMDB_MAX_GROUP);
    if (!isInit) {
        return;
    }
    for (const auto &dirPath : dirPaths) {
        std::experimental::filesystem::remove_all(dirPath);
        if (!std::experimental::filesystem::create_directories(dirPath)) {
            perror("create directory failed");
            abort();
        }
    }
}

bool LogicFile::MMapFile(uint32 sliceno, bool create)
{
    if (m_sliceAddr.size() > sliceno) {
        if (m_sliceAddr[sliceno] != nullptr) {
            return true;
        }
    }
    void *pmemaddr;

#ifdef SIMULATE_MMAP
    pmemaddr = malloc(SLICE_LEN);
#else
    std::string filename = GetFilename(sliceno);
    size_t mmapedLen;
    int isPmem;
    int flags = create ? PMEM_FILE_CREATE : 0;
    int len = create ? SLICE_LEN : 0;
    pmemaddr = pmem_map_file(filename.c_str(), len, flags, PMEM_MODE, &mmapedLen, &isPmem);
    if (pmemaddr == nullptr || mmapedLen != SLICE_LEN) {
        if (!create && sliceno > 0) {
            return false;
        }
        perror("pmem_map_file");
        abort();
    }

    static bool reportSimulate = false;
    if (!isPmem && !reportSimulate) {
        reportSimulate = true;
    }
#endif
    if (m_sliceAddr.size() <= sliceno) {
        if (m_sliceAddr.size() < sliceno) {
            m_sliceAddr.resize(sliceno, nullptr);
        }
        m_sliceAddr.push_back((char *)pmemaddr);
    } else {
        Assert(m_sliceAddr.size() > sliceno);
        m_sliceAddr[sliceno] = (char *)pmemaddr;
    }
    Assert(m_sliceAddr.size() > sliceno);

    return true;
}

void LogicFile::UMMapFile(uint32 sliceno, bool destroy)
{
    if (m_sliceAddr.size() <= sliceno || m_sliceAddr[sliceno] == nullptr) {
        return;
    }
#ifdef SIMULATE_MMAP
    free(slice_addr[sliceno]);
#else
    pmem_unmap(m_sliceAddr[sliceno], SLICE_LEN);
#endif
    m_sliceAddr[sliceno] = nullptr;
    if (destroy) {
        std::string filename = GetFilename(sliceno);
        unlink(filename.c_str());
    }
}

}  // namespace NVMDB