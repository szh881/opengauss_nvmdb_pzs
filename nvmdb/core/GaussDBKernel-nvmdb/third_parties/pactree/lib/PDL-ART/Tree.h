/* -------------------------------------------------------------------------
 *
 * Tree.h
 *
 * Portions Copyright (c) 2023 Huawei Technologies Co.,Ltd.
 * Portions Copyright (c) 2019-2021 Virginia Tech
 * Portions Copyright (c) 2014-2015 Florian Scheibner
 *
 * IDENTIFICATION
 *	  src/gausskernel/storage/nvmdb/core/GaussDBKernel-nvmdb/third_parties/pactree/lib/PDL-ART/Tree.h
 *
 * -------------------------------------------------------------------------
 */
#ifndef ART_ROWEX_TREE_H
#define ART_ROWEX_TREE_H
#include "N.h"
#include "Key.h"
#include "Epoche.h"

using namespace ART;

namespace ART_ROWEX {

class Tree {
public:
    static constexpr uint64_t removeConditionCount = 2;
    static constexpr uint64_t defaultGenId = 3;
    using LoadKeyFunction = void (*)(TID tid, Key &key);
    uint64_t genId{0};
    LoadKeyFunction loadKey{nullptr};

    void Copy(TID *result, std::size_t &resultsFound, std::size_t &resultSize, TID &toContinue, N *node) const;
    void CopyReverse(TID *result, std::size_t &resultsFound, std::size_t &resultSize, TID &toContinue, N *node) const;
    bool FindStart(TID *result, std::size_t &resultsFound, std::size_t &resultSize, const Key &start, TID &toContinue,
                   N *node, N *parentNode, uint32_t level, uint32_t parentLevel) const;

    enum class CheckPrefixResult : uint8_t { MATCH, NO_MATCH, OPTIMISTIC_MATCH };

    enum class CheckPrefixPessimisticResult : uint8_t { MATCH, NO_MATCH, SKIPPED_LEVEL };

    enum class PCCompareResults : uint8_t { SMALLER, EQUAL, BIGGER, SKIPPED_LEVEL };
    enum class PCEqualsResults : uint8_t { PARTIAL_MATCH, BOTH_MATCH, CONTAINED, NO_MATCH, SKIPPED_LEVEL };
    static CheckPrefixResult CheckPrefix(N *n, const Key &k, uint32_t &level);

    static CheckPrefixPessimisticResult CheckPrefixPessimistic(N *n, const Key &k, uint32_t &level,
                                                               uint8_t &nonMatchingKey, Prefix &nonMatchingPrefix,
                                                               LoadKeyFunction loadKey);

    static PCCompareResults CheckPrefixCompare(const N *n, const Key &k, uint32_t &level, LoadKeyFunction loadKey);

    static PCEqualsResults CheckPrefixEquals(const N *n, uint32_t &level, const Key &start, const Key &end,
                                             LoadKeyFunction loadKey);

    void Recover();
    explicit Tree(LoadKeyFunction loadKey);

    Tree(const Tree &) = delete;

    Tree &operator=(const Tree &) = delete;

    Tree(Tree &&t) : root(t.root), loadKey(t.loadKey)
    {}

    Tree &operator=(Tree &&t)
    {
        root = t.root;
        loadKey = t.loadKey;
        return *this;
    }

    ~Tree();

    ThreadInfo GetThreadInfo();

    TID Lookup(const Key &k, ThreadInfo &threadEpocheInfo) const;

    TID LookupNext(const Key &k, ThreadInfo &threadEpocheInfo) const;

    void Insert(const Key &k, TID tid, ThreadInfo &epocheInfo);
    void Remove(const Key &k, TID tid, ThreadInfo &epocheInfo);

#ifdef SYNC
    TID LookupNextwithLock(const Key &start, void **savenode, ThreadInfo &threadEpocheInfo) const;
    bool NodeUnlock(void *savenode, ThreadInfo &threadEpocheInfo) const;
    bool(const Key &k, TID tid, ThreadInfo &epocheInfo, void *locked_node);
#endif

    bool IsEmpty();

private:
    pptr<N> root;

    TID CheckKey(const TID tid, const Key &k) const;

    Epoche epoche{256};
    OpStruct oplogs[10000];
    uint64_t oplogsCount{0};
};

}  // namespace ART_ROWEX
#endif  // ART_ROWEX_TREE_H
