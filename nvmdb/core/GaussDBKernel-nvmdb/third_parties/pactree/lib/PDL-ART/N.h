/* -------------------------------------------------------------------------
 *
 * N.h
 *
 * Portions Copyright (c) 2023 Huawei Technologies Co.,Ltd.
 * Portions Copyright (c) 2019-2021 Virginia Tech
 * Portions Copyright (c) 2014-2015 Florian Scheibner
 *
 * IDENTIFICATION
 *	  src/gausskernel/storage/nvmdb/core/GaussDBKernel-nvmdb/third_parties/pactree/lib/PDL-ART/N.h
 *
 * -------------------------------------------------------------------------
 */
#ifndef ART_ROWEX_N_H
#define ART_ROWEX_N_H
#include <atomic>
#include <cstdint>
#include <cstring>
#include "Key.h"
#include "Epoche.h"
#include "common.h"
#include "pptr.h"
#include "ordo_clock.h"
#include "securec.h"

using TID = uint64_t;

using namespace ART;
namespace ART_ROWEX {
/*
 * SynchronizedTree
 * LockCouplingTree
 * LockCheckFreeReadTree
 * UnsynchronizedTree
 */
inline void SecureRetCheck(errno_t ret)
{
    if (unlikely(ret != EOK)) {
        abort();
    }
}

enum class NTypes : uint8_t { N4 = 0, N16 = 1, N48 = 2, N256 = 3 };

constexpr uint8_t MAX_STORED_PREFIX_LENGTH = 7;
struct Prefix {
    uint8_t prefixCount = 0;
    uint8_t prefix[MAX_STORED_PREFIX_LENGTH];
};
static_assert(sizeof(Prefix) == 8, "Prefix should be 64 bit long");

struct P16BLock {
    uint64_t genId;
    uint64_t lock;
};

class N {
public:
    static constexpr size_t childrenIndexCount = 256;
    static constexpr size_t compactCountOffset = 16;
    static constexpr size_t n48ElementCount = 48;
    static constexpr size_t n16ElementCount = 16;
    static constexpr size_t n4ElementCount = 4;
    void SetType(NTypes type);

    NTypes GetType() const;

    uint32_t GetLevel() const;

    void SetLevel(uint32_t level);

    uint32_t GetCount() const;

    bool IsLocked(uint64_t version) const;

    void WriteLockOrRestart(bool &needRestart, uint32_t genId);

    void LockVersionOrRestart(uint64_t &version, bool &needRestart, uint32_t genId);

    void WriteUnlock();

    uint64_t GetVersion() const;

    /**
     * returns true if node hasn't been changed in between
     */
    bool CheckOrRestart(uint64_t startRead) const;
    bool ReadUnlockOrRestart(uint64_t startRead) const;

    static bool IsObsolete(uint64_t version);

    /**
     * can only be called when node is locked
     */
    void WriteUnlockObsolete()
    {
        typeVersionLockObsolete.fetch_add(0b11);
    }

    static N *GetChild(const uint8_t k, N *node);
    static pptr<N> GetChildPptr(const uint8_t k, N *node);

    static void InsertAndUnlock(N *node, N *parentNode, uint8_t keyParent, uint8_t key, pptr<N> val,
                                ThreadInfo &threadInfo, bool &needRestart, OpStruct *oplog, uint32_t genId);

    static void Change(N *node, uint8_t key, pptr<N> val);

    static void RemoveAndUnlock(N *node, uint8_t key, N *parentNode, uint8_t keyParent, ThreadInfo &threadInfo,
                                bool &needRestart, OpStruct *oplog, uint32_t genId);

    Prefix GetPrefi() const;

    void SetPrefix(const uint8_t *prefix, uint8_t length, bool flush);

    void AddPrefixBefore(N *node, uint8_t key);

    static TID GetLeaf(const N *n);

    static bool IsLeaf(const N *n);

    static N *SetLeaf(TID tid);

    static N *GetAnyChild(const N *n);
    static N *GetAnyChildReverse(const N *n);

    static TID GetAnyChildTid(const N *n);
    static TID GetAnyChildTidReverse(const N *n);

    static void DeleteChildren(N *node);

    static void DeleteNode(N *node);

    static std::tuple<pptr<N>, uint8_t> GetSecondChild(N *node, const uint8_t k);

    template <typename curN, typename biggerN>
    static void InsertGrow(curN *n, N *parentNode, uint8_t keyParent, uint8_t key, pptr<N> val, ThreadInfo &threadInfo,
                           bool &needRestart, OpStruct *oplog, uint32_t genId);

    template <typename curN>
    static void InsertCompact(curN *n, N *parentNode, uint8_t keyParent, uint8_t key, pptr<N> val,
                              ThreadInfo &threadInfo, bool &needRestart, OpStruct *oplog, uint32_t genId);

    template <typename curN, typename smallerN>
    static void RemoveAndShrink(curN *n, N *parentNode, uint8_t keyParent, uint8_t key, ThreadInfo &threadInfo,
                                bool &needRestart, OpStruct *oplog, uint32_t genId);

    static void GetChildren(const N *node, uint8_t start, uint8_t end, std::tuple<uint8_t, N *> children[],
                            uint32_t &childrenCount);
    static N *GetSmallestChild(const N *node, uint8_t start);
    static N *GetLargestChild(const N *node, uint8_t end);
protected:
    N(NTypes type, uint32_t level, const uint8_t *prefix, uint8_t prefixLength) : level(level)
    {
        SetPrefix(prefix, prefixLength, false);
        SetType(type);
    }

    N(NTypes type, uint32_t level, const Prefix &prefi) : prefix(prefi), level(level)
    {
        SetType(type);
    }

    N(const N &) = delete;

    N(N &&) = delete;

    N &operator=(const N &) = delete;

    N &operator=(N &&) = delete;

    // 2b type 60b version 1b lock 1b obsolete
    // 2b type 28b gen id 32b version 1b lock 1b obsolete
    std::atomic<uint64_t> typeVersionLockObsolete{0b100};
    // genID?

    // version 1, unlocked, not obsolete
    std::atomic<Prefix> prefix;
    uint32_t level;
    uint32_t countValues = 0;  // count 2B, compactCount 2B

    static uint64_t ConvertTypeToVersion(NTypes type);
};

class N4 : public N {
public:
    const static int smallestCount = 0;
    std::atomic<uint8_t> keys[4];
    std::atomic<pptr<N>> children[4];

public:
    void *operator new(size_t size, void *location)
    {
        return location;
    }

    N4(uint32_t level, const uint8_t *prefix, uint32_t prefixLength) : N(NTypes::N4, level, prefix, prefixLength)
    {
        int ret = memset_s(keys, sizeof(keys), 0, sizeof(keys));
        SecureRetCheck(ret);
        ret = memset_s(children, sizeof(children), 0, sizeof(children));
        SecureRetCheck(ret);
    }

    N4(uint32_t level, const Prefix &prefi) : N(NTypes::N4, level, prefi)
    {
        int ret = memset_s(keys, sizeof(keys), 0, sizeof(keys));
        SecureRetCheck(ret);
        ret = memset_s(children, sizeof(children), 0, sizeof(children));
        SecureRetCheck(ret);
    }

    bool Insert(uint8_t key, pptr<N> n);
    bool Insert(uint8_t key, pptr<N> n, bool flush);

    template <class NODE>
    void CopyTo(NODE *n) const;

    void Change(uint8_t key, pptr<N> val);

    N *GetChild(const uint8_t k) const;
    pptr<N> GetChildPptr(const uint8_t k) const;

    bool Remove(uint8_t k, bool force);

    N *GetAnyChild() const;
    N *GetAnyChildReverse() const;

    std::tuple<pptr<N>, uint8_t> GetSecondChild(const uint8_t key) const;

    void DeleteChildren();

    void GetChildren(uint8_t start, uint8_t end, std::tuple<uint8_t, N *> *&children, uint32_t &childrenCount) const;
    N *GetSmallestChild(uint8_t start) const;
    N *GetLargestChild(uint8_t end) const;
};

class N16 : public N {
public:
    static constexpr size_t signBit = 128;
    void *operator new(size_t size, void *location)
    {
        return location;
    }

    std::atomic<uint8_t> keys[16];
    std::atomic<pptr<N>> children[16];

    static uint8_t FlipSign(uint8_t keyByte)
    {
        // Flip the sign bit, enables signed SSE comparison of unsigned values, used by Node16
        return keyByte ^ signBit;
    }

    static unsigned Ctz(uint16_t x)
    {
        // Count trailing zeros, only defined for x>0
        return __builtin_ctz(x);
    }

    std::atomic<pptr<N>> *getChildPos(const uint8_t k);

public:
    const static int smallestCount = 3;
    N16(uint32_t level, const uint8_t *prefix, uint32_t prefixLength) : N(NTypes::N16, level, prefix, prefixLength)
    {
        int ret = memset_s(keys, sizeof(keys), 0, sizeof(keys));
        SecureRetCheck(ret);
        ret = memset_s(children, sizeof(children), 0, sizeof(children));
        SecureRetCheck(ret);
    }

    N16(uint32_t level, const Prefix &prefi) : N(NTypes::N16, level, prefi)
    {
        int ret = memset_s(keys, sizeof(keys), 0, sizeof(keys));
        SecureRetCheck(ret);
        ret = memset_s(children, sizeof(children), 0, sizeof(children));
        SecureRetCheck(ret);
    }

    bool Insert(uint8_t key, pptr<N> n);
    bool Insert(uint8_t key, pptr<N> n, bool flush);

    template <class NODE>
    void CopyTo(NODE *n) const;

    void change(uint8_t key, pptr<N> val);

    N *GetChild(const uint8_t k) const;
    pptr<N> getChildPptr(const uint8_t k) const;

    bool Remove(uint8_t k, bool force);

    N *GetAnyChild() const;
    N *GetAnyChildReverse() const;

    void DeleteChildren();

    void GetChildren(uint8_t start, uint8_t end, std::tuple<uint8_t, N *> *&children, uint32_t &childrenCount) const;
    N *GetSmallestChild(uint8_t start) const;
    N *GetLargestChild(uint8_t end) const;
};

class N48 : public N {
    std::atomic<uint8_t> childIndex[256];
    std::atomic<pptr<N>> children[48];
    // std::atomic<N *> children[48];

    pptr<N> GetChildPptrByIndex(const uint8_t k) const;

public:
    const static int smallestCount = 12;
    void *operator new(size_t size, void *location)
    {
        return location;
    }

    static const uint8_t emptyMarker = 48;

    N48(uint32_t level, const uint8_t *prefix, uint32_t prefixLength) : N(NTypes::N48, level, prefix, prefixLength)
    {
        int ret = memset_s(childIndex, sizeof(childIndex), emptyMarker, sizeof(childIndex));
        SecureRetCheck(ret);
        ret = memset_s(children, sizeof(children), 0, sizeof(children));
        SecureRetCheck(ret);
    }

    N48(uint32_t level, const Prefix &prefi) : N(NTypes::N48, level, prefi)
    {
        int ret = memset_s(childIndex, sizeof(childIndex), emptyMarker, sizeof(childIndex));
        SecureRetCheck(ret);
        ret = memset_s(children, sizeof(children), 0, sizeof(children));
        SecureRetCheck(ret);
    }

    bool Insert(uint8_t key, pptr<N> n);
    bool Insert(uint8_t key, pptr<N> n, bool flush);

    template <class NODE>
    void CopyTo(NODE *n) const;

    void Change(uint8_t key, pptr<N> val);

    N *GetChild(const uint8_t k) const;
    pptr<N> GetChildPptr(const uint8_t k) const;

    bool Remove(uint8_t k, bool force);

    N *GetAnyChild() const;
    N *GetAnyChildReverse() const;

    void DeleteChildren();

    void GetChildren(uint8_t start, uint8_t end, std::tuple<uint8_t, N *> *&children, uint32_t &childrenCount) const;
    N *GetSmallestChild(uint8_t start) const;
    N *GetLargestChild(uint8_t end) const;
};

class N256 : public N {
    std::atomic<pptr<N>> children[256];

    pptr<N> GetChildPptrByIndex(const uint8_t k) const;

public:
    const static int smallestCount = 38;
    void *operator new(size_t size, void *location)
    {
        return location;
    }

    N256(uint32_t level, const uint8_t *prefix, uint32_t prefixLength) : N(NTypes::N256, level, prefix, prefixLength)
    {
        int ret = memset_s(children, sizeof(children), '\0', sizeof(children));
        SecureRetCheck(ret);
    }

    N256(uint32_t level, const Prefix &prefi) : N(NTypes::N256, level, prefi)
    {
        int ret = memset_s(children, sizeof(children), '\0', sizeof(children));
        SecureRetCheck(ret);
    }

    bool Insert(uint8_t key, pptr<N> val);
    bool Insert(uint8_t key, pptr<N> val, bool flush);

    template <class NODE>
    void CopyTo(NODE *n) const;

    void Change(uint8_t key, pptr<N> n);

    N *GetChild(const uint8_t k) const;
    pptr<N> GetChildPptr(const uint8_t k) const;

    bool Remove(uint8_t k, bool force);

    N *GetAnyChild() const;
    N *GetAnyChildReverse() const;

    void DeleteChildren();

    void GetChildren(uint8_t start, uint8_t end, std::tuple<uint8_t, N *> *&children, uint32_t &childrenCount) const;
    N *GetSmallestChild(uint8_t start) const;
    N *GetLargestChild(uint8_t end) const;
};
}  // namespace ART_ROWEX
#endif  // ART_ROWEX_N_H
