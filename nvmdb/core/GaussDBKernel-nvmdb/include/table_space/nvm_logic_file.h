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
 * nvm_logic_file.h
 *
 * IDENTIFICATION
 *   src/gausskernel/storage/nvmdb/core/GaussDBKernel-nvmdb/include/table_space/nvm_logic_file.h
 * -------------------------------------------------------------------------
 */
#ifndef NVMDB_LOGIC_FILE_H
#define NVMDB_LOGIC_FILE_H

#include <string>
#include <vector>
#include <mutex>
#include <unistd.h>

#include "nvm_types.h"
#include "nvm_block.h"
#include "nvm_utils.h"

namespace NVMDB {

const char *const DELIMITER = ";";

extern std::vector<std::string> g_dirPaths;

extern uint32 g_dirPathNum;

void ParseDirectoryConfig(const char *dir_names, bool is_init = false, uint32 &_dir_path_num = g_dirPathNum,
                          std::vector<std::string> &_dir_paths = g_dirPaths);

/*
 *  一个逻辑上的大文件，向外展示连续的页号；给定一个页号会翻译成对应的虚拟地址。
 *  内部实现会切成多个slice，每个slice是一个物理文件，mmap到虚拟地址空间中。支持不同长度的slice。
 *  支持 create、mount、unmount、extend、truncate操作。
 *
 *  Heap的tablespace和 Undo的segment都可以继承这个类，使用其文件管理、地址翻译的功能。
 */
class LogicFile {
public:
    size_t SLICE_LEN;
    size_t MAX_SLICE_NUM;
    size_t SLICE_BLOCKS;

    /*  slice_addr 数组初始化的时候就设够足够大，否则当数组扩展的时候，会影响并发的读 */
    LogicFile(const char *dir, const char *name, size_t slice_len, size_t max_slice_num)
        : m_spcname(name), SLICE_LEN(slice_len), MAX_SLICE_NUM(max_slice_num), SLICE_BLOCKS(slice_len / NVM_BLCKSZ)
    {
        ParseDirectoryConfig(dir, false, m_dirPathNum, m_dirPaths);
        m_sliceAddr.reserve(max_slice_num);
    }

    uint32 SliceNumber()
    {
        return m_sliceAddr.size();
    }

    virtual void Create()
    {
        Assert(m_sliceAddr.size() == 0);
        MMapFile(0, true);
    }

    virtual void Mount()
    {
        bool exist = true;
        for (int i = 0; i < MAX_SLICE_NUM; i++) {
            exist = MMapFile(i, false);
            if (!exist) {
                break;
            }
        }
        MMapFile(0, false);
    }

    virtual void UnMount()
    {
        for (uint32 i = 0; i < m_sliceAddr.size(); i++) {
            UMMapFile(i);
        }
        m_sliceAddr.clear();
    }

    void extend(uint32 pageno)
    {
        MMapFile(pageno / SLICE_BLOCKS, true);
    }

    void punch(uint32 start_sliceno, uint32 end_sliceno)
    {
        Assert(start_sliceno < end_sliceno);
        for (uint32 i = start_sliceno; i < end_sliceno; i++) {
            UMMapFile(i, true);
        }
    }

    void truncate()
    {}
    void Drop()
    {}

    /* 将偏移翻译成虚拟地址 */
    char *relpoint(PointerOffset ptr)
    {
        uint32 blkno = ptr / NVM_BLCKSZ;
        return (char *)(RelpointOfPageno(blkno) + (ptr % NVM_BLCKSZ));
    }

    /* 语法糖，页号的虚拟地址 */
    char *RelpointOfPageno(uint32 blkno)
    {
        uint32 sliceno = blkno / SLICE_BLOCKS;
        Assert(sliceno < m_sliceAddr.size() && m_sliceAddr[sliceno] != nullptr);
        return m_sliceAddr[sliceno] + ((uint64)(blkno % SLICE_BLOCKS) * NVM_BLCKSZ);
    }
protected:
    std::string m_spcname;
    std::vector<char *> m_sliceAddr;
    std::mutex m_spcMtx;

    uint32 m_dirPathNum;
    std::vector<std::string> m_dirPaths;

    bool MMapFile(uint32 sliceno, bool create);
    void UMMapFile(uint32 sliceno, bool destroy = false);

private:
    inline std::string GetFilename(int sliceno)
    {
        return m_dirPaths[sliceno % m_dirPathNum] + "/" + m_spcname + '.' + std::to_string(sliceno);
    }
};

}  // namespace NVMDB
#endif  // NVMDB_LOGIC_FILE_H