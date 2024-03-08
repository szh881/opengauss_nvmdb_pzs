/*
 * Portions Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 * Portions Copyright (c) 2010-2012, PostgreSQL Global Development Group
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
 * mot_fdw.cpp
 *    NVM Foreign Data Wrapper implementation.
 *
 * IDENTIFICATION
 *    src/gausskernel/storage/nvm/fdw_adapter/nvm_fdw.cpp
 *
 * -------------------------------------------------------------------------
 */

#include "access/reloptions.h"
#include "pgxc/locator.h"
#include "access/transam.h"
#include "postgres.h"
#include "catalog/pg_foreign_table.h"
#include "nodes/parsenodes.h"
#include "commands/copy.h"
#include "commands/defrem.h"
#include "commands/explain.h"
#include "commands/vacuum.h"
#include "commands/tablecmds.h"
#include "foreign/fdwapi.h"
#include "foreign/foreign.h"
#include "miscadmin.h"
#include "nodes/nodes.h"
#include "nodes/nodeFuncs.h"
#include "optimizer/cost.h"
#include "optimizer/pathnode.h"
#include "optimizer/planmain.h"
#include "optimizer/restrictinfo.h"
#include "utils/memutils.h"
#include "utils/rel.h"
#include "utils/date.h"
#include "utils/syscache.h"
#include "utils/partitionkey.h"
#include "catalog/heap.h"
#include "optimizer/var.h"
#include "optimizer/clauses.h"
#include "optimizer/pathnode.h"
#include "optimizer/subselect.h"
#include "catalog/pg_collation.h"
#include "catalog/pg_database.h"
#include "catalog/pg_operator.h"
#include "catalog/pg_proc.h"
#include "mb/pg_wchar.h"
#include "utils/lsyscache.h"
#include "miscadmin.h"
#include "parser/parsetree.h"
#include "access/sysattr.h"
#include "tcop/utility.h"
#include "postmaster/bgwriter.h"
#include "storage/lmgr.h"
#include "storage/ipc.h"

#include <map>
#include <limits>
#include <cmath>
#include "nvm_dbcore.h"
#include "nvm_table.h"
#include "nvm_transaction.h"
#include "nvm_access.h"
#include "nvmdb_thread.h"
#include "nvm_fdw_internal.h"
#include "nvm_tuple.h"
#include "nvm_index_tuple.h"
#include "nvm_index_access.h"
#include "nvm_heap_space.h"

#ifdef LOG
#undef LOG
#define LOG 15
#endif

extern "C" Datum nvm_fdw_handler(PG_FUNCTION_ARGS);
extern "C" Datum nvm_fdw_validator(PG_FUNCTION_ARGS);

void InitNvmThread();

static inline void NVMAssert(bool cond)
{
    if (unlikely(!cond)) {
        abort();
    }
}

namespace NVMDB {
union FloatConvT {
    float mV;
    uint32_t mR;
    uint8_t mC[4];
};

union DoubleConvT {
    double mV;
    uint64_t mR;
    uint16_t mC[4];
};

struct NvmRowId {
    RowId m_rowId;
    uint16 m_reserve = 0x1234;
} __attribute__((packed));

class NvmTableMap {
public:
    auto Find(Oid oid)
    {
        return m_map.find(oid);
    }

    auto Insert(const std::pair<Oid, Table *> &el)
    {
        el.second->RefCountInc();
        return m_map.insert(el);
    }

    auto Erase(std::map<Oid, Table *>::iterator iter)
    {
        iter->second->RefCountDec();
        return m_map.erase(iter);
    }

    void Clear()
    {
        auto iter = m_map.begin();
        while (iter != m_map.end()) {
            iter = this->Erase(iter);
        }
    }

    auto Begin()
    {
        return m_map.begin();
    }

    auto End()
    {
        return m_map.end();
    }

private:
    std::map<Oid, Table *> m_map;
};

thread_local NvmTableMap g_nvmdbTableLocal;
NvmTableMap g_nvmdbTable;
static std::mutex g_tableMutex;

static const char *g_nvmdbErrcodeStr[] = {
    "success",
    "input para error",
    "unsupported col type",
    "no memory",
    "table not found",
    "index not found",
    "col not found",
    "index type not support",
    "index not support nullable col",
    "index size over limit",
    "col size over limit",
    "index not support expr"
};

static inline const char *NvmGetErrcodeStr(NVM_ERRCODE err)
{
    if (likely(err < NVM_ERRCODE::NVM_ERRCODE_INVALID)) {
        return g_nvmdbErrcodeStr[static_cast<int>(err)];
    } else {
        NVMAssert(false);
    }
}

static inline bool NvmIsTxnInAbortState(Transaction *txn)
{
    return txn->GetTrxStatus() == TX_WAIT_ABORT;
}

static inline void NvmRaiseAbortTxnError()
{
    ereport(ERROR, (errmodule(MOD_NVM), errcode(ERRCODE_IN_FAILED_SQL_TRANSACTION),
                    errmsg("current transaction is aborted, "
                           "commands ignored until end of transaction block, firstChar[%c]",
                           u_sess->proc_cxt.firstChar),
                    errdetail("Please perform rollback")));
}

static ColumnType GetNVMType(Oid &typoid)
{
    ColumnType type;
    switch (typoid) {
        case CHAROID:
            type = COL_TYPE_CHAR;
            break;
        case INT1OID:
        case BOOLOID:
            type = COL_TYPE_TINY;
            break;
        case INT2OID:
            type = COL_TYPE_SHORT;
            break;
        case INT4OID:
            type = COL_TYPE_INT;
            break;
        case INT8OID:
            type = COL_TYPE_LONG;
            break;
        case DATEOID:
            type = COL_TYPE_DATE;
            break;
        case TIMEOID:
            type = COL_TYPE_TIME;
            break;
        case TIMESTAMPOID:
            type = COL_TYPE_TIMESTAMP;
            break;
        case TIMESTAMPTZOID:
            type = COL_TYPE_TIMESTAMPTZ;
            break;
        case INTERVALOID:
            type = COL_TYPE_INTERVAL;
            break;
        case TINTERVALOID:
            type = COL_TYPE_TINTERVAL;
            break;
        case TIMETZOID:
            type = COL_TYPE_TIMETZ;
            break;
        case FLOAT4OID:
            type = COL_TYPE_FLOAT;
            break;
        case FLOAT8OID:
            type = COL_TYPE_DOUBLE;
            break;
        case NUMERICOID:
            type = COL_TYPE_DECIMAL;
            break;
        case VARCHAROID:
        case BPCHAROID:
        case TEXTOID:
            type = COL_TYPE_VARCHAR;
            break;
        default:
            type = COL_TYPE_INVALID;
    }
    return type;
}

void NvmThreadlocalTableMapClear()
{
    g_nvmdbTableLocal.Clear();
}

void NvmDropTable(Oid oid)
{
    auto iter = g_nvmdbTableLocal.Find(oid);
    if (iter != g_nvmdbTableLocal.End()) {
        g_nvmdbTableLocal.Erase(iter);
    }

    std::lock_guard<std::mutex> lock_guard(g_tableMutex);
    iter = g_nvmdbTable.Find(oid);
    if (iter != g_nvmdbTable.End()) {
        iter->second->Dropped();
        g_nvmdbTable.Erase(iter);
    }

    g_heapSpace->DropTable(oid);

    return;
}

Table *NvmGetTableByOid(Oid oid)
{
    auto iter = g_nvmdbTableLocal.Find(oid);
    if (iter != g_nvmdbTableLocal.End()) {
        if (unlikely(iter->second->IsDropped())) {
            g_nvmdbTableLocal.Erase(iter);
            return nullptr;
        }
        return iter->second;
    } else {
        std::lock_guard<std::mutex> lock_guard(g_tableMutex);
        iter = g_nvmdbTable.Find(oid);
        if (iter != g_nvmdbTable.End()) {
            g_nvmdbTableLocal.Insert(std::make_pair(oid, iter->second));
            return iter->second;
        }

        // Rebuild the table desc.
        uint32 tableSeg = g_heapSpace->SearchTable(oid);
        if (tableSeg == 0) {
            return nullptr;
        }
        Relation rel = RelationIdGetRelation(oid);
        TableDesc tableDesc;
        if (!TableDescInit(&tableDesc, rel->rd_att->natts)) {
            return nullptr;
        }
        for (int n = 0; n < rel->rd_att->natts; ++n) {
            tableDesc.col_desc[n].m_colType = GetNVMType(rel->rd_att->attrs[n].atttypid);
            tableDesc.col_desc[n].m_colOid = rel->rd_att->attrs[n].atttypid;
            if (rel->rd_att->attrs[n].attlen == -1) {
                if (tableDesc.col_desc[n].m_colOid == VARCHAROID || tableDesc.col_desc[n].m_colOid == BPCHAROID) {
                    tableDesc.col_desc[n].m_colLen = rel->rd_att->attrs[n].atttypmod;
                } else if (tableDesc.col_desc[n].m_colOid == NUMERICOID) {
                    tableDesc.col_desc[n].m_colLen = DECIMAL_MAX_SIZE;
                } else if (tableDesc.col_desc[n].m_colOid == TEXTOID) {
                    tableDesc.col_desc[n].m_colLen = MAX_VARCHAR_LEN;
                } else {
                    NVMAssert(false);
                }
            } else {
                tableDesc.col_desc[n].m_colLen = rel->rd_att->attrs[n].attlen;
            }
            tableDesc.col_desc[n].m_colOffset = tableDesc.row_len;
            tableDesc.col_desc[n].m_isNotNull = rel->rd_att->attrs[n].attnotnull;
            char *name = rel->rd_att->attrs[n].attname.data;
            errno_t errorno = memcpy_s(tableDesc.col_desc[n].m_colName, sizeof(name), name, sizeof(name));
            securec_check_c(errorno, "\0", "\0");
            tableDesc.row_len += tableDesc.col_desc[n].m_colLen;
        }
        Table *table = table = new (std::nothrow) Table(oid, tableDesc);
        table->Mount(tableSeg);

        // Rebuild the index desc.
        ListCell *l;
        const ColumnDesc *tabledesc = table->GetColDesc();
        if (rel->rd_indexvalid == 0) {
            List *indexes = RelationGetIndexList(rel);
        }
        foreach (l, rel->rd_indexlist) {
            uint64 index_len = 0;
            Oid indexOid = lfirst_oid(l);
            NVMIndex *index = new (std::nothrow) NVMIndex(indexOid);
            Relation indexRel = RelationIdGetRelation(indexOid);
            uint32 colCount = indexRel->rd_index->indnatts;
            IndexColumnDesc *indexDesc = IndexDescCreate(colCount);
            if (false == index->SetNumTableFields(colCount)) {
                return nullptr;
            }
            for (uint32 i = 0; i < colCount; i++) {
                // in opengauss, colid start from 1 but nvmdb start from 0.
                indexDesc[i].m_colId = indexRel->rd_index->indkey.values[i] - 1;
                index->FillBitmap(indexDesc[i].m_colId);
            }
            RelationClose(indexRel);
            InitIndexDesc(indexDesc, tabledesc, colCount, index_len);
            index->SetIndexDesc(indexDesc, colCount, index_len);
            table->AddIndex(index);
        }

        RelationClose(rel);
        g_nvmdbTable.Insert(std::make_pair(oid, table));
        g_nvmdbTableLocal.Insert(std::make_pair(oid, table));
        return table;
    }
}

static inline Table *NvmGetTableByOidWrapper(Oid oid)
{
    Table *table = NvmGetTableByOid(oid);
    if (unlikely(table == nullptr)) {
        ereport(ERROR, (errcode(ERRCODE_T_R_SERIALIZATION_FAILURE),
                        errmsg("Get nvm table fail(%d)!", NVM_ERRCODE::NVM_ERRCODE_TABLE_NOT_FOUND)));
    }
    return table;
}

static inline void NVMVarLenFieldType(Form_pg_type typeDesc, Oid typoid, int32_t colLen, int16 *typeLen)
{
    bool isBlob = false;
    if (typeDesc->typlen < 0) {
        *typeLen = colLen;
        switch (typeDesc->typstorage) {
            case 'p':
                break;
            case 'x':
            case 'm':
                if (typoid == NUMERICOID) {
                    *typeLen = DECIMAL_MAX_SIZE;
                    break;
                }
            case 'e':
                if (typoid == TEXTOID)
                    *typeLen = colLen = MAX_VARCHAR_LEN;
                if (colLen > MAX_VARCHAR_LEN || colLen < 0) {
                    NVMAssert(false);
                } else {
                    isBlob = true;
                }
                break;
            default:
                break;
        }
    }

    return;
}

static NVM_ERRCODE GetTypeInfo(const ColumnDef *colDef, ColumnType &type, int16 &typeLen, Oid &typoid)
{
    NVM_ERRCODE res = NVM_ERRCODE::NVM_SUCCESS;
    Type tup;
    Form_pg_type typeDesc;
    int32_t colLen;

    tup = typenameType(nullptr, colDef->typname, &colLen);
    typeDesc = ((Form_pg_type)GETSTRUCT(tup));
    typoid = HeapTupleGetOid(tup);
    typeLen = typeDesc->typlen;

    NVMVarLenFieldType(typeDesc, typoid, colLen, &typeLen);

    type = GetNVMType(typoid);
    if (type == COL_TYPE_INVALID) {
        res = NVM_ERRCODE::NVM_ERRCODE_UNSUPPORTED_COL_TYPE;
    }

    if (tup) {
        ReleaseSysCache(tup);
    }

    return res;
}

static Datum GetTypeMax(Oid type)
{
    int ret;
    Datum data = 0;
    float fdata;
    double ddata;

    switch (type) {
        case CHAROID:
        case INT1OID:
        case VARCHAROID:
        case BPCHAROID:
        case TEXTOID:
            data = std::numeric_limits<unsigned char>::max();
            break;
        case BOOLOID:
            data = 1;
            break;
        case INT2OID:
            data = std::numeric_limits<short>::max();
            break;
        case INT4OID:
            data = std::numeric_limits<int>::max();
            break;
        case INT8OID:
            data = std::numeric_limits<long>::max();
            break;
        case TIMEOID:
            data = std::numeric_limits<unsigned long>::max();
            break;
        case FLOAT4OID:
            fdata = std::numeric_limits<float>::max();
            ret = memcpy_s(&data, sizeof(fdata), &fdata, sizeof(fdata));
            SecureRetCheck(ret);
            break;
        case FLOAT8OID:
            ddata = std::numeric_limits<double>::max();
            ret = memcpy_s(&data, sizeof(ddata), &ddata, sizeof(ddata));
            SecureRetCheck(ret);
            break;
        case NUMERICOID:
        default:
            NVMAssert(false);
    }

    return data;
}

static Datum GetTypeMin(Oid type)
{
    int ret;
    Datum data = 0;
    float fdata;
    double ddata;
    switch (type) {
        case CHAROID:
        case INT1OID:
        case VARCHAROID:
        case BPCHAROID:
        case TEXTOID:
            data = std::numeric_limits<unsigned char>::lowest();
            break;
        case BOOLOID:
            data = 1;
            break;
        case INT2OID:
            data = std::numeric_limits<short>::lowest();
            break;
        case INT4OID:
            data = std::numeric_limits<int>::lowest();
            break;
        case INT8OID:
            data = std::numeric_limits<long>::lowest();
            break;
        case TIMEOID:
            data = std::numeric_limits<unsigned long>::lowest();
            break;
        case FLOAT4OID:
            fdata = std::numeric_limits<float>::lowest();
            ret = memcpy_s(&data, sizeof(fdata), &fdata, sizeof(fdata));
            SecureRetCheck(ret);
            break;
        case FLOAT8OID:
            ddata = std::numeric_limits<double>::lowest();
            ret = memcpy_s(&data, sizeof(ddata), &ddata, sizeof(ddata));
            SecureRetCheck(ret);
            break;
        case NUMERICOID:
        default:
            NVMAssert(false);
    }

    return data;
}

NVM_ERRCODE CreateTable(CreateForeignTableStmt *stmt, ::TransactionId tid)
{
    TableDesc tableDesc;
    ListCell *cell = nullptr;
    Table *table = nullptr;
    NVM_ERRCODE ret = NVM_ERRCODE::NVM_SUCCESS;
    uint32 colIndex = 0;

    if (list_length(stmt->base.tableElts) > NVMDB_TUPLE_MAX_COL_COUNT) {
        ret = NVM_ERRCODE::NVM_ERRCODE_COL_COUNT_EXC_LIMIT;
        goto OUT;
    }

    if (!TableDescInit(&tableDesc, list_length(stmt->base.tableElts))) {
        ret = NVM_ERRCODE::NVM_ERRCODE_NO_MEM;
        goto OUT;
    }

    foreach (cell, stmt->base.tableElts) {
        int16 typeLen = 0;
        ColumnType colType;
        ColumnDef *colDef = (ColumnDef *)lfirst(cell);
        ColumnDesc *colDesc = &(tableDesc.col_desc[colIndex]);
        errno_t rc = EOK;

        if (colDef == nullptr || colDef->typname == nullptr) {
            ret = NVM_ERRCODE::NVM_ERRCODE_INPUT_PARA_ERROR;
            break;
        }

        Oid typoid = InvalidOid;
        ret = GetTypeInfo(colDef, colType, typeLen, typoid);
        if (ret != NVM_ERRCODE::NVM_SUCCESS) {
            break;
        }

        colDesc->m_colLen = typeLen;
        colDesc->m_colType = colType;
        colDesc->m_colOid = typoid;
        colDesc->m_colOffset = tableDesc.row_len;
        colDesc->m_isNotNull = colDef->is_not_null;
        rc = strcpy_s(colDesc->m_colName, NVM_MAX_COLUMN_NAME_LEN, colDef->colname);
        securec_check(rc, "\0", "\0");

        tableDesc.row_len += typeLen;
        colIndex++;
    }

    if (likely(ret == NVM_ERRCODE::NVM_SUCCESS)) {
        table = new (std::nothrow) Table(stmt->base.relation->foreignOid, tableDesc);
        if (likely(table != nullptr)) {
            g_tableMutex.lock();
            g_nvmdbTable.Insert(std::make_pair(stmt->base.relation->foreignOid, table));
            g_tableMutex.unlock();
            g_nvmdbTableLocal.Insert(std::make_pair(stmt->base.relation->foreignOid, table));
            uint32 tableSeg = table->CreateSegment();
            g_heapSpace->CreateTable(TableSegMetaData{stmt->base.relation->foreignOid, tableSeg});
        } else {
            ret = NVM_ERRCODE::NVM_ERRCODE_NO_MEM;
        }
    }

OUT:
    if (ret != NVM_ERRCODE::NVM_SUCCESS) {
        TableDescDestroy(&tableDesc);
        ereport(ERROR, (errmodule(MOD_NVM), errcode(ERRCODE_INVALID_COLUMN_DEFINITION),
                        errmsg("NVM create table fail:%s!", NvmGetErrcodeStr(ret))));
    }

    return ret;
}

Transaction *NVMGetCurrentTrxContext()
{
    if (!t_thrd.nvmdb_init) {
        InitNvmThread();
    }

    if (u_sess->nvm_cxt.trx == nullptr) {
        u_sess->nvm_cxt.trx = new Transaction();
        NVMAssert(u_sess->nvm_cxt.trx != nullptr);
        u_sess->nvm_cxt.trx->Begin();
    }
    return u_sess->nvm_cxt.trx;
}

void NVMStateFree(NVMFdwState *nvmState)
{
    if (nvmState != nullptr) {
        delete nvmState->mIter;

        if (nvmState->mAttrsUsed != nullptr) {
            pfree(nvmState->mAttrsUsed);
        }

        if (nvmState->mAttrsModified != nullptr) {
            pfree(nvmState->mAttrsModified);
        }

        list_free(nvmState->mConst.mParentExprList);
        list_free(nvmState->mConstPara.mParentExprList);

        pfree(nvmState);
    }

    return;
}

/* Convertors */
inline static void PGNumericToNVM(const Numeric n, DecimalSt &d)
{
    int sign = NUMERIC_SIGN(n);

    d.m_hdr.m_flags = 0;
    d.m_hdr.m_flags |=
        (sign == NUMERIC_POS ? DECIMAL_POSITIVE
                             : (sign == NUMERIC_NEG ? DECIMAL_NEGATIVE : ((sign == NUMERIC_NAN) ? DECIMAL_NAN : 0)));
    d.m_hdr.m_ndigits = NUMERIC_NDIGITS(n);
    d.m_hdr.m_scale = NUMERIC_DSCALE(n);
    d.m_hdr.m_weight = NUMERIC_WEIGHT(n);
    d.m_round = 0;
    if (d.m_hdr.m_ndigits > 0) {
        errno_t erc = memcpy_s(d.m_digits, DECIMAL_MAX_SIZE - sizeof(DecimalSt), (void *)NUMERIC_DIGITS(n),
                               d.m_hdr.m_ndigits * sizeof(NumericDigit));
        securec_check(erc, "\0", "\0");
    }
}

inline static Numeric NVMNumericToPG(DecimalSt *d)
{
    NumericVar v;

    v.ndigits = d->m_hdr.m_ndigits;
    v.dscale = d->m_hdr.m_scale;
    v.weight = (int)(int16_t)(d->m_hdr.m_weight);
    v.sign = (d->m_hdr.m_flags & DECIMAL_POSITIVE
                  ? NUMERIC_POS
                  : (d->m_hdr.m_flags & DECIMAL_NEGATIVE ? NUMERIC_NEG
                                                         : ((d->m_hdr.m_flags & DECIMAL_NAN) ? DECIMAL_NAN : 0)));
    v.buf = (NumericDigit *)&d->m_round;
    v.digits = (NumericDigit *)d->m_digits;

    return makeNumeric(&v);
}

void NVMColInitData(RAMTuple &tuple, uint16 colIndex, Datum datum, Oid atttypi)
{
    switch (atttypi) {
        case BYTEAOID:
        case TEXTOID:
        case VARCHAROID:
        case CLOBOID:
        case BPCHAROID: {
            bytea *txt = DatumGetByteaP(datum);
            uint32 size = VARSIZE(txt);
            size -= sizeof(uint32);
            errno_t ret = memcpy_s(txt, sizeof(uint32), &size, sizeof(uint32));
            SecureRetCheck(ret);
            tuple.SetCol(colIndex, (char *)txt, size + sizeof(uint32));
            SET_VARSIZE(txt, size + sizeof(uint32));

            if ((char *)datum != (char *)txt) {
                pfree(txt);
            }
            break;
        }
        case NUMERICOID: {
            Numeric n = DatumGetNumeric(datum);
            char buf[DECIMAL_MAX_SIZE];
            DecimalSt *d = (DecimalSt *)buf;

            if (NUMERIC_NDIGITS(n) > DECIMAL_MAX_DIGITS) {
                ereport(ERROR, (errmodule(MOD_NVM), errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
                                errmsg("Value exceeds maximum precision: %d", NVM_NUMERIC_MAX_PRECISION)));
                break;
            }
            PGNumericToNVM(n, *d);
            tuple.SetCol(colIndex, (char *)d, DECIMAL_SIZE(d));
            break;
        }
        case INTERVALOID:
        case TINTERVALOID:
        case TIMETZOID:
            tuple.SetCol(colIndex, (char *)datum);
            break;
        default:
            tuple.SetCol(colIndex, (char *)&datum);
            break;
    }
}

void NVMColUpdateData(RAMTuple &tuple, uint16 colIndex, Datum datum, Oid atttypi, uint64 len)
{
    switch (atttypi) {
        case BYTEAOID:
        case TEXTOID:
        case VARCHAROID:
        case CLOBOID:
        case BPCHAROID: {
            bytea *txt = DatumGetByteaP(datum);
            uint32 size = VARSIZE(txt);
            size -= sizeof(uint32);
            errno_t ret = memcpy_s(txt, sizeof(uint32), &size, sizeof(uint32));
            SecureRetCheck(ret);
            tuple.UpdateColInc(colIndex, (char *)txt, size + sizeof(uint32));
            SET_VARSIZE(txt, size + sizeof(uint32));

            if ((char *)datum != (char *)txt) {
                pfree(txt);
            }
            break;
        }
        case NUMERICOID: {
            Numeric n = DatumGetNumeric(datum);
            char buf[DECIMAL_MAX_SIZE];
            DecimalSt *d = (DecimalSt *)buf;

            if (NUMERIC_NDIGITS(n) > DECIMAL_MAX_DIGITS) {
                ereport(ERROR, (errmodule(MOD_NVM), errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
                                errmsg("Value exceeds maximum precision: %d", NVM_NUMERIC_MAX_PRECISION)));
                break;
            }
            PGNumericToNVM(n, *d);
            tuple.UpdateColInc(colIndex, (char *)d, DECIMAL_SIZE(d));
            break;
        }
        case INTERVALOID:
        case TINTERVALOID:
        case TIMETZOID:
            tuple.UpdateColInc(colIndex, (char *)datum, len);
            break;
        default:
            tuple.UpdateColInc(colIndex, (char *)&datum, len);
            break;
    }

    return;
}

void NVMInsertTuple2AllIndex(Transaction *trx, Table *table, RAMTuple *tuple, RowId rowId)
{
    uint32 count = table->GetIndexCount();

    for (uint32 i = 0; i < count; i++) {
        NVMIndex *index = table->GetIndex(i);
        NVMAssert(index != nullptr);
        DRAMIndexTuple indexTuple(table->GetColDesc(), index->GetIndexDesc(), index->GetColCount(), index->GetRowLen());

        indexTuple.ExtractFromTuple(tuple);

        IndexInsert(trx, index, &indexTuple, rowId);
    }

    return;
}

void NVMDeleteTupleFromAllIndex(Transaction *trx, Table *table, RAMTuple *tuple, RowId rowId)
{
    uint32 count = table->GetIndexCount();

    for (uint32 i = 0; i < count; i++) {
        NVMIndex *index = table->GetIndex(i);
        NVMAssert(index != nullptr);

        DRAMIndexTuple indexTuple(table->GetColDesc(), index->GetIndexDesc(), index->GetColCount(), index->GetRowLen());

        indexTuple.ExtractFromTuple(tuple);

        IndexDelete(trx, index, &indexTuple, rowId);
    }

    return;
}

void NVMInsertTuple2Index(Transaction *trx, Table *table, NVMIndex *index, RAMTuple *tuple, RowId rowId)
{
    DRAMIndexTuple indexTuple(table->GetColDesc(), index->GetIndexDesc(), index->GetColCount(), index->GetRowLen());

    indexTuple.ExtractFromTuple(tuple);

    IndexInsert(trx, index, &indexTuple, rowId);

    return;
}

void NVMDeleteTupleFromIndex(Transaction *trx, Table *table, NVMIndex *index, RAMTuple *tuple, RowId rowId)
{
    DRAMIndexTuple indexTuple(table->GetColDesc(), index->GetIndexDesc(), index->GetColCount(), index->GetRowLen());

    indexTuple.ExtractFromTuple(tuple);

    IndexDelete(trx, index, &indexTuple, rowId);

    return;
}

TupleTableSlot *NVMExecForeignInsertImplement(EState *estate, ResultRelInfo *resultRelInfo, TupleTableSlot *slot,
                                              TupleTableSlot *planSlot)
{
    Table *table = nullptr;
    RowId rowId;
    Transaction *trx = NVMDB::NVMGetCurrentTrxContext();
    NVMFdwState *nvmState = (NVMFdwState *)resultRelInfo->ri_FdwState;
    TupleDesc tupdesc = slot->tts_tupleDescriptor;
    uint64_t i = 0;
    uint64_t j = 1;

    if (NvmIsTxnInAbortState(trx)) {
        NvmRaiseAbortTxnError();
    }

    table = NvmGetTableByOidWrapper(RelationGetRelid(resultRelInfo->ri_RelationDesc));
    if (nvmState == nullptr) {
        nvmState = reinterpret_cast<NVMFdwState *>(palloc0(sizeof(NVMFdwState)));
        nvmState->mCurrTxn = trx;
        nvmState->mTable = table;
        nvmState->mNumAttrs = RelationGetNumberOfAttributes(resultRelInfo->ri_RelationDesc);
        nvmState->mConst.mCost = std::numeric_limits<double>::max();
        nvmState->mConstPara.mCost = std::numeric_limits<double>::max();
        resultRelInfo->ri_FdwState = nvmState;
    }

    RAMTuple tuple(table->GetColDesc(), table->GetRowLen());

    for (; i < nvmState->mNumAttrs; i++, j++) {
        bool isnull = false;
        Datum value = heap_slot_getattr(slot, j, &isnull);

        if (!isnull) {
            NVMColInitData(tuple, i, value, tupdesc->attrs[i].atttypid);
            tuple.SetNull(i, false);
        } else {
            tuple.SetNull(i);
        }
    }

    rowId = HeapInsert(trx, table, &tuple);

    NVMInsertTuple2AllIndex(trx, table, &tuple, rowId);

    return slot;
}

void NVMFillSlotByCol(Table *table, RAMTuple *tuple, const Form_pg_attribute attr, Datum *value, bool *isNull)
{
    size_t len = 0;
    *isNull = false;
    uint16 index = attr->attnum - 1;

    switch (attr->atttypid) {
        case VARCHAROID:
        case BPCHAROID:
        case TEXTOID:
        case CLOBOID:
        case BYTEAOID: {
            char *data = tuple->GetCol(index);
            bytea *result = (bytea *)data;
            uint32 len = *(uint32 *)data;
            SET_VARSIZE(result, len + VARHDRSZ);
            *value = PointerGetDatum(result);
            break;
        }
        case NUMERICOID: {
            DecimalSt *d = (DecimalSt *)(tuple->GetCol(index));
            *value = NumericGetDatum(NVMNumericToPG(d));
            break;
        }
        case INTERVALOID:
        case TINTERVALOID:
        case TIMETZOID:
            *value = (Datum)(tuple->GetCol(index));
            break;
        default:
            tuple->GetCol(index, (char *)value);
            break;
    }
}

void NVMFillSlotByTuple(TupleTableSlot *slot, Table *table, RAMTuple *tuple)
{
    TupleDesc tupdesc = slot->tts_tupleDescriptor;
    uint64 cols = table->GetColCount();

    for (uint64 i = 0; i < cols; i++) {
        slot->tts_isnull[i] = tuple->IsNull(i);
        if (slot->tts_isnull[i]) {
            continue;
        }
        switch (tupdesc->attrs[i].atttypid) {
            case VARCHAROID:
            case BPCHAROID:
            case TEXTOID:
            case CLOBOID:
            case BYTEAOID: {
                char *data = tuple->GetCol(i);
                bytea *result = (bytea *)data;
                uint32 len = *(uint32 *)data;
                SET_VARSIZE(result, len + VARHDRSZ);
                slot->tts_values[i] = PointerGetDatum(result);
                break;
            }
            case NUMERICOID: {
                DecimalSt *d = (DecimalSt *)(tuple->GetCol(i));
                slot->tts_values[i] = NumericGetDatum(NVMNumericToPG(d));
                break;
            }
            case INTERVALOID:
            case TINTERVALOID:
            case TIMETZOID:
                slot->tts_values[i] = (Datum)(tuple->GetCol(i));
                break;
            default:
                tuple->GetCol(i, (char *)&(slot->tts_values[i]));
                break;
        }
    }

    return;
}

inline bool NVMIsNotEqualOper(OpExpr *op)
{
    switch (op->opno) {
        case INT48NEOID:
        case BooleanNotEqualOperator:
        case 402:
        case INT8NEOID:
        case INT84NEOID:
        case INT4NEOID:
        case INT2NEOID:
        case 531:
        case INT24NEOID:
        case INT42NEOID:
        case 561:
        case 567:
        case 576:
        case 608:
        case 644:
        case FLOAT4NEOID:
        case 630:
        case 5514:
        case 643:
        case FLOAT8NEOID:
        case 713:
        case 812:
        case 901:
        case BPCHARNEOID:
        case 1071:
        case DATENEOID:
        case 1109:
        case 1551:
        case FLOAT48NEOID:
        case FLOAT84NEOID:
        case 1321:
        case 1331:
        case 1501:
        case 1586:
        case 1221:
        case 1202:
        case NUMERICNEOID:
        case 1785:
        case 1805:
        case INT28NEOID:
        case INT82NEOID:
        case 1956:
        case 3799:
        case TIMESTAMPNEOID:
        case 2350:
        case 2363:
        case 2376:
        case 2389:
        case 2539:
        case 2545:
        case 2973:
        case 3517:
        case 3630:
        case 3677:
        case 2989:
        case 3883:
        case 5551:
            return true;

        default:
            return false;
    }
}

inline List *NVMBitmapSerialize(List *result, uint8_t *bitmap, int16_t len)
{
    result = lappend(result, makeConst(INT4OID, -1, InvalidOid, sizeof(int),
                                       static_cast<int>(FDW_LIST_TYPE::FDW_LIST_BITMAP), false, true));
    for (int i = 0; i < len; i++)
        result = lappend(result, makeConst(INT1OID, -1, InvalidOid, 1, Int8GetDatum(bitmap[i]), false, true));

    return result;
}

inline void NVMBitmapDeSerialize(uint8_t *bitmap, int16_t len, ListCell **cell)
{
    if (cell != nullptr && *cell != nullptr) {
        int type = ((Const *)lfirst(*cell))->constvalue;
        if (type == static_cast<int>(FDW_LIST_TYPE::FDW_LIST_BITMAP)) {
            *cell = lnext(*cell);
            for (int i = 0; i < len; i++) {
                bitmap[i] = (uint8_t)((Const *)lfirst(*cell))->constvalue;
                *cell = lnext(*cell);
            }
        }
    }
}

NVMFdwState *NVMInitializeFdwState(void *fdwState, List **fdwExpr, uint64_t exTableID)
{
    NVMFdwState *state = reinterpret_cast<NVMFdwState *>(palloc0(sizeof(NVMFdwState)));
    List *values = (List *)fdwState;

    state->mAllocInScan = true;
    state->mForeignTableId = exTableID;
    state->mConst.mCost = std::numeric_limits<double>::max();
    state->mConstPara.mCost = std::numeric_limits<double>::max();
    if (list_length(values) > 0) {
        ListCell *cell = list_head(values);
        int type = ((Const *)lfirst(cell))->constvalue;
        if (type != static_cast<int>(FDW_LIST_TYPE::FDW_LIST_STATE)) {
            return state;
        }
        cell = lnext(cell);
        state->mCmdOper = (CmdType)((Const *)lfirst(cell))->constvalue;
        cell = lnext(cell);
        state->mForeignTableId = ((Const *)lfirst(cell))->constvalue;
        cell = lnext(cell);
        state->mNumAttrs = ((Const *)lfirst(cell))->constvalue;
        cell = lnext(cell);
        state->mCtidNum = ((Const *)lfirst(cell))->constvalue;
        cell = lnext(cell);
        state->mNumExpr = ((Const *)lfirst(cell))->constvalue;
        cell = lnext(cell);

        state->mConst.mIndex = (NVMIndex *)((Const *)lfirst(cell))->constvalue;
        cell = lnext(cell);
        state->mConst.mMatchCount = (uint32)((Const *)lfirst(cell))->constvalue;
        cell = lnext(cell);
        for (uint j = 0; j < NVM_MAX_KEY_COLUMNS; j++) {
            state->mConst.mOper[j] = (KEY_OPER)((Const *)lfirst(cell))->constvalue;
            cell = lnext(cell);
        }

        int len = BITMAP_GETLEN(state->mNumAttrs);
        state->mAttrsUsed = reinterpret_cast<uint8_t *> palloc0(len);
        state->mAttrsModified = reinterpret_cast<uint8_t *> palloc0(len);
        NVMBitmapDeSerialize(state->mAttrsUsed, len, &cell);

        if (fdwExpr != NULL && *fdwExpr != NULL) {
            ListCell *c = NULL;
            int i = 0;

            state->mConst.mParentExprList = NULL;

            foreach (c, *fdwExpr) {
                if (i < state->mNumExpr) {
                    i++;
                    continue;
                } else {
                    state->mConst.mParentExprList = lappend(state->mConst.mParentExprList, lfirst(c));
                }
            }

            *fdwExpr = list_truncate(*fdwExpr, state->mNumExpr);
        }
    }

    return state;
}

void *NVMSerializeFdwState(NVMFdwState *state)
{
    List *result = NULL;

    result = lappend(result, makeConst(INT4OID, -1, InvalidOid, sizeof(int),
                                       static_cast<int>(FDW_LIST_TYPE::FDW_LIST_STATE), false, true));
    result =
        lappend(result, makeConst(INT4OID, -1, InvalidOid, sizeof(int), Int32GetDatum(state->mCmdOper), false, true));
    result = lappend(
        result, makeConst(INT4OID, -1, InvalidOid, sizeof(int), Int32GetDatum(state->mForeignTableId), false, true));
    result =
        lappend(result, makeConst(INT4OID, -1, InvalidOid, sizeof(int), Int32GetDatum(state->mNumAttrs), false, true));
    result =
        lappend(result, makeConst(INT4OID, -1, InvalidOid, sizeof(int), Int32GetDatum(state->mCtidNum), false, true));
    result =
        lappend(result, makeConst(INT2OID, -1, InvalidOid, sizeof(short), Int16GetDatum(state->mNumExpr), false, true));

    result = lappend(result, makeConst(INT8OID, -1, InvalidOid, sizeof(long long), Int64GetDatum(state->mConst.mIndex),
                                       false, true));
    result = lappend(
        result, makeConst(INT4OID, -1, InvalidOid, sizeof(int), Int32GetDatum(state->mConst.mMatchCount), false, true));
    for (uint j = 0; j < NVM_MAX_KEY_COLUMNS; j++) {
        result = lappend(result, makeConst(INT4OID, -1, InvalidOid, sizeof(int), UInt32GetDatum(state->mConst.mOper[j]),
                                           false, true));
    }

    int len = BITMAP_GETLEN(state->mNumAttrs);
    result = NVMBitmapSerialize(result, state->mAttrsUsed, len);

    NVMStateFree(state);

    return result;
}

inline bool NVMGetKeyOperation(OpExpr *op, KEY_OPER &oper)
{
    switch (op->opno) {
        case FLOAT8EQOID:
        case FLOAT4EQOID:
        case INT2EQOID:
        case INT4EQOID:
        case INT8EQOID:
        case INT24EQOID:
        case INT42EQOID:
        case INT84EQOID:
        case INT48EQOID:
        case INT28EQOID:
        case INT82EQOID:
        case FLOAT48EQOID:
        case FLOAT84EQOID:
        case 5513:  // INT1EQ
        case BPCHAREQOID:
        case TEXTEQOID:
        case 92:    // CHAREQ
        case 2536:  // timestampVStimestamptz
        case 2542:  // timestamptzVStimestamp
        case 2347:  // dateVStimestamp
        case 2360:  // dateVStimestamptz
        case 2373:  // timestampVSdate
        case 2386:  // timestamptzVSdate
        case TIMESTAMPEQOID:
            oper = KEY_OPER::READ_KEY_EXACT;
            break;
        case FLOAT8LTOID:
        case FLOAT4LTOID:
        case INT2LTOID:
        case INT4LTOID:
        case INT8LTOID:
        case INT24LTOID:
        case INT42LTOID:
        case INT84LTOID:
        case INT48LTOID:
        case INT28LTOID:
        case INT82LTOID:
        case FLOAT48LTOID:
        case FLOAT84LTOID:
        case 5515:  // INT1LT
        case 1058:  // BPCHARLT
        case 631:   // CHARLT
        case TEXTLTOID:
        case 2534:  // timestampVStimestamptz
        case 2540:  // timestamptzVStimestamp
        case 2345:  // dateVStimestamp
        case 2358:  // dateVStimestamptz
        case 2371:  // timestampVSdate
        case 2384:  // timestamptzVSdate
        case TIMESTAMPLTOID:
            oper = KEY_OPER::READ_KEY_BEFORE;
            break;
        case FLOAT8LEOID:
        case FLOAT4LEOID:
        case INT2LEOID:
        case INT4LEOID:
        case INT8LEOID:
        case INT24LEOID:
        case INT42LEOID:
        case INT84LEOID:
        case INT48LEOID:
        case INT28LEOID:
        case INT82LEOID:
        case FLOAT48LEOID:
        case FLOAT84LEOID:
        case 5516:  // INT1LE
        case 1059:  // BPCHARLE
        case 632:   // CHARLE
        case 665:   // TEXTLE
        case 2535:  // timestampVStimestamptz
        case 2541:  // timestamptzVStimestamp
        case 2346:  // dateVStimestamp
        case 2359:  // dateVStimestamptz
        case 2372:  // timestampVSdate
        case 2385:  // timestamptzVSdate
        case TIMESTAMPLEOID:
            oper = KEY_OPER::READ_KEY_OR_PREV;
            break;
        case FLOAT8GTOID:
        case FLOAT4GTOID:
        case INT2GTOID:
        case INT4GTOID:
        case INT8GTOID:
        case INT24GTOID:
        case INT42GTOID:
        case INT84GTOID:
        case INT48GTOID:
        case INT28GTOID:
        case INT82GTOID:
        case FLOAT48GTOID:
        case FLOAT84GTOID:
        case 5517:       // INT1GT
        case 1060:       // BPCHARGT
        case 633:        // CHARGT
        case TEXTGTOID:  // TEXTGT
        case 2538:       // timestampVStimestamptz
        case 2544:       // timestamptzVStimestamp
        case 2349:       // dateVStimestamp
        case 2362:       // dateVStimestamptz
        case 2375:       // timestampVSdate
        case 2388:       // timestamptzVSdate
        case TIMESTAMPGTOID:
            oper = KEY_OPER::READ_KEY_AFTER;
            break;
        case FLOAT8GEOID:
        case FLOAT4GEOID:
        case INT2GEOID:
        case INT4GEOID:
        case INT8GEOID:
        case INT24GEOID:
        case INT42GEOID:
        case INT84GEOID:
        case INT48GEOID:
        case INT28GEOID:
        case INT82GEOID:
        case FLOAT48GEOID:
        case FLOAT84GEOID:
        case 5518:  // INT1GE
        case 1061:  // BPCHARGE
        case 634:   // CHARGE
        case 667:   // TEXTGE
        case 2537:  // timestampVStimestamptz
        case 2543:  // timestamptzVStimestamp
        case 2348:  // dateVStimestamp
        case 2361:  // dateVStimestamptz
        case 2374:  // timestampVSdate
        case 2387:  // timestamptzVSdate
        case TIMESTAMPGEOID:
            oper = KEY_OPER::READ_KEY_OR_NEXT;
            break;
        default:
            oper = KEY_OPER::READ_INVALID;
            break;
    }

    return (oper != KEY_OPER::READ_INVALID);
}

inline void RevertKeyOperation(KEY_OPER &oper)
{
    if (oper == KEY_OPER::READ_KEY_BEFORE) {
        oper = KEY_OPER::READ_KEY_AFTER;
    } else if (oper == KEY_OPER::READ_KEY_OR_PREV) {
        oper = KEY_OPER::READ_KEY_OR_NEXT;
    } else if (oper == KEY_OPER::READ_KEY_AFTER) {
        oper = KEY_OPER::READ_KEY_BEFORE;
    } else if (oper == KEY_OPER::READ_KEY_OR_NEXT) {
        oper = KEY_OPER::READ_KEY_OR_PREV;
    }
    return;
}

bool NvmMatchIndexs(NVMFdwState *state, uint32 col, NvmMatchIndexArr *matchArray, Expr *expr, Expr *parent,
                    KEY_OPER oper)
{
    bool result = false;
    uint32 indexCount = state->mTable->GetIndexCount();

    for (uint32 i = 0; i < indexCount; i++) {
        NVMIndex *index = state->mTable->GetIndex(i);
        if (index != nullptr && index->IsFieldPresent(col)) {
            if (matchArray->m_idx[i] == nullptr) {
                matchArray->m_idx[i] = (NvmMatchIndex *)palloc0(sizeof(NvmMatchIndex));
                matchArray->m_idx[i]->Init();
                matchArray->m_idx[i]->m_ix = index;
            }

            result |= matchArray->m_idx[i]->SetIndexColumn(state, col, oper, expr, parent);
        }
    }

    return result;
}

bool IsNVMExpr(RelOptInfo *baserel, NVMFdwState *state, Expr *expr, Expr **result, NvmMatchIndexArr *matchArray)
{
    bool isNvm = false;

    switch (expr->type) {
        case T_Const: {
            if (result != nullptr)
                *result = expr;
            isNvm = true;
            break;
        }
        case T_Var: {
            if (result != nullptr)
                *result = expr;
            isNvm = true;
            break;
        }
        case T_Param: {
            if (result != nullptr)
                *result = expr;
            isNvm = true;
            break;
        }
        case T_FuncExpr: {
            FuncExpr *func = (FuncExpr *)expr;

            if (func->funcformat == COERCE_IMPLICIT_CAST || func->funcformat == COERCE_EXPLICIT_CAST) {
                isNvm = IsNVMExpr(baserel, state, (Expr *)linitial(func->args), result, matchArray);
            } else if (list_length(func->args) == 0) {
                isNvm = true;
            }

            break;
        }
        case T_RelabelType: {
            isNvm = IsNVMExpr(baserel, state, ((RelabelType *)expr)->arg, result, matchArray);
            break;
        }
        case T_OpExpr: {
            KEY_OPER oper;
            OpExpr *op = (OpExpr *)expr;
            Expr *l = (Expr *)linitial(op->args);

            if (list_length(op->args) == 1) {
                isNvm = IsNVMExpr(baserel, state, l, &l, matchArray);
                break;
            }

            Expr *r = (Expr *)lsecond(op->args);
            isNvm = IsNVMExpr(baserel, state, l, &l, matchArray);
            isNvm &= IsNVMExpr(baserel, state, r, &r, matchArray);
            if (result != nullptr && isNvm) {
                if (IsA(l, Var) && IsA(r, Var) && ((Var *)l)->varno == ((Var *)r)->varno) {
                    isNvm = false;
                }
                break;
            }

            if (isNvm && NVMGetKeyOperation(op, oper)) {
                Var *v = nullptr;
                Expr *e = nullptr;

                if (IsA(l, Var)) {
                    if (!IsA(r, Var)) {
                        v = (Var *)l;
                        e = r;
                    } else {
                        if (((Var *)l)->varno == ((Var *)r)->varno) {  // same relation
                            return false;
                        } else if (bms_is_member(((Var *)l)->varno, baserel->relids)) {
                            v = (Var *)l;
                            e = r;
                        } else {
                            v = (Var *)r;
                            e = l;
                            RevertKeyOperation(oper);
                        }
                    }
                } else if (IsA(r, Var)) {
                    v = (Var *)r;
                    e = l;
                    RevertKeyOperation(oper);
                } else {
                    isNvm = false;
                    break;
                }

                return NvmMatchIndexs(state, v->varattno - 1, matchArray, e, expr, oper);
            }
            break;
        }
        default: {
            isNvm = false;
            break;
        }
    }

    return isNvm;
}

bool NVMIsSameOper(KEY_OPER op1, KEY_OPER op2)
{
    bool res = true;
    if (op1 == op2) {
        return res;
    }

    switch (op1) {
        case KEY_OPER::READ_KEY_OR_NEXT:
        case KEY_OPER::READ_KEY_AFTER: {
            switch (op2) {
                case KEY_OPER::READ_KEY_OR_NEXT:
                case KEY_OPER::READ_KEY_AFTER:
                    break;
                default:
                    res = false;
                    break;
            }
            break;
        }
        case KEY_OPER::READ_KEY_OR_PREV:
        case KEY_OPER::READ_KEY_BEFORE: {
            switch (op2) {
                case KEY_OPER::READ_KEY_OR_PREV:
                case KEY_OPER::READ_KEY_BEFORE:
                    break;
                default:
                    res = false;
                    break;
            }
            break;
        }
        default:
            res = false;
            break;
    }

    return res;
}

static inline bool IsNVMCurrentIndexBetter(double sumCostOrg, uint32 colCountOrg, double sumCostCur, uint32 colCountCur)
{
    if ((colCountCur > colCountOrg) || ((colCountCur == colCountOrg) && (sumCostCur < sumCostOrg))) {
        return true;
    } else {
        return false;
    }
}

static constexpr uint32 NVM_BEST_MUL_FACTOR = 1000;

NVMIndex *NvmGetBestIndex(NVMFdwState *state, NvmMatchIndexArr *matchArray, NVMFdwConstraint *constraint)
{
    uint32 indexCount = state->mTable->GetIndexCount();
    int bestIndex = -1;
    uint32 colCount = 0;
    uint32 maxColCount = 0;
    uint32 indexColCount = 0;
    double cost = 10;
    double sumCost = 0;
    double maxCost = std::numeric_limits<double>::max();

    for (int i = 0; i < indexCount; i++) {
        NvmMatchIndex *matchIndex = matchArray->m_idx[i];
        if (matchIndex != nullptr) {
            NVMIndex *m_ix = matchIndex->m_ix;
            const IndexColumnDesc *desc = m_ix->GetIndexDesc();

            if (matchIndex->m_colMatch[0] == nullptr) {
                continue;
            }

            colCount = 1;
            indexColCount = m_ix->GetColCount();

            for (int j = 1; j < indexColCount; j++) {
                if (matchIndex->m_colMatch[j] != nullptr) {
                    colCount++;
                } else {
                    break;
                }
            }

            sumCost = colCount + NVM_BEST_MUL_FACTOR * (indexColCount - colCount);
            if (IsNVMCurrentIndexBetter(maxCost, maxColCount, sumCost, colCount)) {
                maxColCount = colCount;
                bestIndex = i;
                maxCost = sumCost;
            }
        }
    }

    if (bestIndex != -1) {
        NvmMatchIndex *matchIndex = matchArray->m_idx[bestIndex];
        constraint->mIndex = matchIndex->m_ix;
        constraint->mMatchCount = maxColCount;
        constraint->mStartupCost = 0.01;
        constraint->mCost = maxCost;
        errno_t ret = memcpy_s(constraint->mOper, sizeof(KEY_OPER) * NVM_MAX_KEY_COLUMNS, matchIndex->m_opers,
                               sizeof(KEY_OPER) * NVM_MAX_KEY_COLUMNS);
        SecureRetCheck(ret);
        list_free(constraint->mParentExprList);
        list_free(constraint->mExprList);
        constraint->mParentExprList = nullptr;
        constraint->mExprList = nullptr;
        for (int j = 0; j < maxColCount; j++) {
            constraint->mParentExprList = lappend(constraint->mParentExprList, matchIndex->m_parentColMatch[j]);
            constraint->mExprList = lappend(constraint->mExprList, matchIndex->m_colMatch[j]);
        }
    }

    return constraint->mIndex;
}

void NVMVarcharToIndexKey(Datum datum, DRAMIndexTuple *tuple, uint32 colIndex, uint64 maxLen)
{
    bool noValue = false;

    bytea *txt = DatumGetByteaP(datum);
    size_t size = VARSIZE(txt);
    char *src = VARDATA(txt);

    size -= VARHDRSZ;
    NVMAssert(size < maxLen);

    tuple->SetCol(colIndex, src, (uint64)size);

    if ((char *)datum != (char *)txt) {
        pfree(txt);
    }
}

void NVMIndexTupleWriteData(Oid colType, DRAMIndexTuple *tuple, NVMFdwState *festate, uint32 colIndex, Datum datum)
{
    NVMIndex *index = festate->mConst.mIndex;
    const IndexColumnDesc *desc = index->GetIndexDesc();
    NVMAssert(colIndex < index->GetColCount());
    uint32 len = desc[colIndex].m_colLen;

    switch (colType) {
        case BYTEAOID:
        case TEXTOID:
        case VARCHAROID:
        case CLOBOID:
        case BPCHAROID: {
            char buffer[len];
            *(uint32 *)buffer = len - sizeof(uint32);
            errno_t ret = memset_s(buffer + sizeof(uint32), len - sizeof(uint32), datum, len - sizeof(uint32));
            SecureRetCheck(ret);
            tuple->SetCol(colIndex, buffer);
            break;
        }
        default:
            tuple->SetCol(colIndex, (char *)&datum);
            break;
    }

    return;
}

void NVMIndexTupleFillMax(Oid colType, DRAMIndexTuple *tuple, NVMFdwState *festate, uint32 colIndex)
{
    Datum datum = GetTypeMax(colType);
    NVMIndexTupleWriteData(colType, tuple, festate, colIndex, datum);
    return;
}

void NVMIndexTupleFillMin(Oid colType, DRAMIndexTuple *tuple, NVMFdwState *festate, uint32 colIndex)
{
    Datum datum = GetTypeMin(colType);
    NVMIndexTupleWriteData(colType, tuple, festate, colIndex, datum);
    return;
}

void NVMFillExactValue2IndexTuple(Oid datumType, Datum datum, Oid colType, DRAMIndexTuple *tuple, NVMFdwState *festate,
                                  uint32 colIndex)
{
    NVMIndex *index = festate->mConst.mIndex;
    const IndexColumnDesc *desc = index->GetIndexDesc();
    NVMAssert(colIndex < index->GetColCount());
    uint32 len = desc[colIndex].m_colLen;

    switch (colType) {
        case BYTEAOID:
        case TEXTOID:
        case VARCHAROID:
        case CLOBOID:
        case BPCHAROID: {
            bytea *txt = DatumGetByteaP(datum);
            uint64 size = VARSIZE(txt);
            *(uint32 *)txt = size - sizeof(uint32);
            tuple->SetCol(colIndex, (char *)txt, size);
            SET_VARSIZE(txt, size);
            if ((char *)datum != (char *)txt) {
                pfree(txt);
            }
            break;
        }
        case FLOAT4OID:
            if (datumType == FLOAT8OID) {
                DoubleConvT dc;
                FloatConvT fc;
                dc.mR = static_cast<uint64>(datum);
                fc.mV = static_cast<float>(dc.mV);
                uint64 u = static_cast<uint64>(fc.mR);
                tuple->SetCol(colIndex, reinterpret_cast<char *>(&u));
            } else {
                tuple->SetCol(colIndex, reinterpret_cast<char *>(&datum));
            }
            break;
        default:
            tuple->SetCol(colIndex, reinterpret_cast<char *>(&datum));
            break;
    }
}

void NVMFillExactValue2IndexTuples(Oid datumType, Datum datum, Oid colType, DRAMIndexTuple *begin, DRAMIndexTuple *end,
                                   NVMFdwState *festate, uint32 colIndex)
{
    NVMIndex *index = festate->mConst.mIndex;
    const IndexColumnDesc *desc = index->GetIndexDesc();
    NVMAssert(colIndex < index->GetColCount());
    uint32 len = desc[colIndex].m_colLen;
    KEY_OPER oper = festate->mConst.mOper[colIndex];

    switch (oper) {
        case KEY_OPER::READ_KEY_EXACT:
            NVMFillExactValue2IndexTuple(datumType, datum, colType, begin, festate, colIndex);
            end->Copy(begin, colIndex);
            break;
        case KEY_OPER::READ_KEY_OR_NEXT:
        case KEY_OPER::READ_KEY_AFTER:
            NVMFillExactValue2IndexTuple(datumType, datum, colType, begin, festate, colIndex);
            NVMIndexTupleFillMax(colType, end, festate, colIndex);
            break;
        case KEY_OPER::READ_KEY_OR_PREV:
        case KEY_OPER::READ_KEY_BEFORE:
            NVMFillExactValue2IndexTuple(datumType, datum, colType, end, festate, colIndex);
            NVMIndexTupleFillMin(colType, begin, festate, colIndex);
            break;
        default:
            NVMAssert(false);
    }

    return;
}

NVMDB::NVMIndexIter *NvmIndexIterOpen(ForeignScanState *node, NVMFdwState *festate)
{
    NVMDB::NVMIndexIter *result = nullptr;
    NVMDB::Table *table = festate->mTable;
    ForeignScan *fscan = (ForeignScan *)node->ss.ps.plan;
    ExprContext *econtext = nullptr;
    List *execExprs = nullptr;
    bool isNull = false;
    Relation rel = node->ss.ss_currentRelation;
    TupleDesc desc = rel->rd_att;
    NVMIndex *index = festate->mConst.mIndex;
    uint32 indexColCount = index->GetColCount();
    const IndexColumnDesc *indexDesc = index->GetIndexDesc();
    bool reverse = false;

    execExprs = (List *)ExecInitExpr((Expr *)fscan->fdw_exprs, (PlanState *)node);
    NVMAssert(festate->mConst.mMatchCount == list_length(execExprs));
    econtext = node->ss.ps.ps_ExprContext;
    DRAMIndexTuple indexTupleBegin(table->GetColDesc(), index->GetIndexDesc(), index->GetColCount(),
                                   index->GetRowLen());
    DRAMIndexTuple indexTupleEnd(table->GetColDesc(), index->GetIndexDesc(), index->GetColCount(), index->GetRowLen());

    for (int i = 0; i < indexColCount; i++) {
        Oid colType = desc->attrs[indexDesc[i].m_colId].atttypid;
        if (i < festate->mConst.mMatchCount) {
            ExprState *expr = (ExprState *)list_nth(execExprs, i);
            Datum val = ExecEvalExpr((ExprState *)(expr), econtext, &isNull, nullptr);
            if (likely(!isNull)) {
                NVMFillExactValue2IndexTuples(expr->resultType, val, colType, &indexTupleBegin, &indexTupleEnd, festate,
                                              i);
            } else {
                NVMAssert(0);
            }
        } else {
            NVMIndexTupleFillMin(colType, &indexTupleBegin, festate, i);
            NVMIndexTupleFillMax(colType, &indexTupleEnd, festate, i);
        }
    }

    auto ss = GetIndexLookupSnapshot(festate->mCurrTxn);
    result = index->GenerateIter(&indexTupleBegin, &indexTupleEnd, ss, 0, false);
    NVMAssert(result != nullptr);

    return result;
}

NvmFdwIter *NvmGetIter(ForeignScanState *node, NVMFdwState *festate)
{
    if (festate->mIter == nullptr) {
        if (festate->mConst.mIndex != nullptr) {
            festate->mIter = new (std::nothrow) NvmFdwIndexIter(NvmIndexIterOpen(node, festate));
        } else {
            festate->mIter = new (std::nothrow) NvmFdwSeqIter(NVMDB::HeapUpperRowId(festate->mTable));
        }
        NVMAssert(festate->mIter != nullptr);
    }

    return festate->mIter;
}

void NVMIndexRestore(Table *table, NVMIndex *index)
{
    Transaction *trx = NVMGetCurrentTrxContext();
    RowId rowId = 0;
    HAM_STATUS status;
    RAMTuple tuple(table->GetColDesc(), table->GetRowLen());

    for (rowId = 0; rowId < HeapUpperRowId(table); rowId++) {
        status = HeapRead(trx, table, rowId, &tuple);
        if (status == HAM_SUCCESS) {
            NVMInsertTuple2Index(trx, table, index, &tuple, rowId);
        }
    }

    return;
}

void NVMIndexDeleteAllData(Table *table, NVMIndex *index)
{
    Transaction *trx = NVMGetCurrentTrxContext();
    RowId rowId = 0;
    HAM_STATUS status;
    RAMTuple tuple(table->GetColDesc(), table->GetRowLen());

    for (rowId = 0; rowId < HeapUpperRowId(table); rowId++) {
        status = HeapRead(trx, table, rowId, &tuple);
        if (status == HAM_SUCCESS) {
            NVMDeleteTupleFromIndex(trx, table, index, &tuple, rowId);
        }
    }
    return;
}

NVM_ERRCODE CreateIndex(IndexStmt *stmt, ::TransactionId tid)
{
    IndexColumnDesc *indexDesc;
    NVMIndex *index = nullptr;
    ListCell *lc = nullptr;
    NVM_ERRCODE result = NVM_ERRCODE::NVM_SUCCESS;
    uint32 i = 0;
    uint32 colCount = list_length(stmt->indexParams);
    uint64 index_len = 0;

    do {
        Table *table = NvmGetTableByOid(stmt->relation->foreignOid);
        if (unlikely(table == nullptr)) {
            result = NVM_ERRCODE::NVM_ERRCODE_TABLE_NOT_FOUND;
            break;
        }

        index = new (std::nothrow) NVMIndex(stmt->indexOid);
        if (unlikely(index == nullptr)) {
            result = NVM_ERRCODE::NVM_ERRCODE_NO_MEM;
            break;
        }

        indexDesc = IndexDescCreate(colCount);
        if (unlikely(indexDesc == nullptr)) {
            result = NVM_ERRCODE::NVM_ERRCODE_NO_MEM;
            break;
        }

        if (unlikely(!index->SetNumTableFields(table->GetColCount()))) {
            result = NVM_ERRCODE::NVM_ERRCODE_NO_MEM;
            break;
        }

        foreach (lc, stmt->indexParams) {
            IndexElem *ielem = (IndexElem *)lfirst(lc);

            if (ielem->expr != nullptr) {
                result = NVM_ERRCODE::NVM_ERRCODE_INDEX_NOT_SUPPORT_EXPR;
                goto CREATE_INDEX_OUT;
            }

            uint32 colid = table->GetColIdByName((ielem->name != nullptr ? ielem->name : ielem->indexcolname));
            if (colid == InvalidColId) {
                result = NVM_ERRCODE::NVM_ERRCODE_COL_NOT_FOUND;
                goto CREATE_INDEX_OUT;
            }

            if (!IsIndexTypeSupported(table->GetColDesc(colid)->m_colType)) {
                result = NVM_ERRCODE::NVM_ERRCODE_INDEX_TYPE_NOT_SUPPORT;
                goto CREATE_INDEX_OUT;
            }

            if (!table->ColIsNotNull(colid)) {
                result = NVM_ERRCODE::NVM_ERRCODE_INDEX_NOT_SUPPORT_NULL;
                goto CREATE_INDEX_OUT;
            }

            indexDesc[i].m_colId = colid;

            index->FillBitmap(colid);

            i++;
        }

        Assert(i == colCount);

        const ColumnDesc *tabledesc = table->GetColDesc();

        InitIndexDesc(indexDesc, tabledesc, colCount, index_len);

        if (index_len > KEY_DATA_LENGTH) {
            result = NVM_ERRCODE::NVM_ERRCODE_INDEX_SIZE_EXC_LIMIT;
            goto CREATE_INDEX_OUT;
        }

        index->SetIndexDesc(indexDesc, colCount, index_len);

        table->AddIndex(index);

        NVMIndexRestore(table, index);
    } while (0);

CREATE_INDEX_OUT:
    if (result != NVM_ERRCODE::NVM_SUCCESS) {
        delete index;
        IndexDescDelete(indexDesc);
        ereport(ERROR, (errcode(ERRCODE_T_R_SERIALIZATION_FAILURE),
                        errmsg("NVM create index fail:%s!", NvmGetErrcodeStr(result))));
    }

    return result;
}

NVM_ERRCODE DropIndex(DropForeignStmt *stmt, ::TransactionId tid)
{
    NVM_ERRCODE result = NVM_ERRCODE::NVM_SUCCESS;
    size_t i = 0;

    do {
        Table *table = NvmGetTableByOid(stmt->reloid);
        if (table == nullptr) {
            result = NVM_ERRCODE::NVM_ERRCODE_TABLE_NOT_FOUND;
            break;
        }

        NVMIndex *index = table->DelIndex(stmt->indexoid);
        if (index == nullptr) {
            result = NVM_ERRCODE::NVM_ERRCODE_INDEX_NOT_FOUND;
            break;
        }

        NVMIndexDeleteAllData(table, index);

        delete index;
    } while (0);

    if (result != NVM_ERRCODE::NVM_SUCCESS) {
        ereport(ERROR, (errcode(ERRCODE_T_R_SERIALIZATION_FAILURE), errmsg("NVM delete index fail(%d)!", result)));
    }

    return result;
}

NVM_ERRCODE DropTable(DropForeignStmt *stmt, ::TransactionId tid)
{
    NVM_ERRCODE result = NVM_ERRCODE::NVM_SUCCESS;

    NvmDropTable(stmt->reloid);

    return result;
}

}  // namespace NVMDB

Datum nvm_fdw_validator(PG_FUNCTION_ARGS)
{
    List *otherOptions = NIL;

    /*
     * Now apply the core COPY code's validation logic for more checks.
     */
    ProcessCopyOptions(NULL, true, otherOptions);

    PG_RETURN_VOID();
}

static void NVMAddForeignUpdateTargets(Query *parsetree, RangeTblEntry *targetRte, Relation targetRelation)
{
    Var *var;
    const char *attrname;
    TargetEntry *tle;

    /* Make a Var representing the desired value */
    var = makeVar(parsetree->resultRelation, SelfItemPointerAttributeNumber, TIDOID, -1, InvalidOid, 0);

    /* Wrap it in a resjunk TLE with the right name ... */
    attrname = NVM_REC_TID_NAME;

    tle = makeTargetEntry((Expr *)var, list_length(parsetree->targetList) + 1, pstrdup(attrname), true);

    /* ... and add it to the query's targetlist */
    parsetree->targetList = lappend(parsetree->targetList, tle);

    return;
}

static constexpr double NVMDB_ESTIMATED_QUANTITY = 100000;
static constexpr double NVMDB_START_UP_COST = 0.1;
static void NVMGetForeignRelSize(PlannerInfo *root, RelOptInfo *baserel, Oid foreigntableid)
{
    NVMFdwState *festate = nullptr;
    ForeignTable *ftable = GetForeignTable(foreigntableid);
    Relation rel = RelationIdGetRelation(ftable->relid);
    ListCell *t = NULL;
    NVMDB::Table *table = nullptr;
    bool needWholeRow = false;
    TupleDesc desc;
    ListCell *lc = nullptr;
    Bitmapset *attrs = nullptr;

    table = NVMDB::NvmGetTableByOidWrapper(RelationGetRelid(rel));
    festate = reinterpret_cast<NVMFdwState *>(palloc0(sizeof(NVMFdwState)));
    festate->mConst.mCost = std::numeric_limits<double>::max();
    festate->mConstPara.mCost = std::numeric_limits<double>::max();
    festate->mCurrTxn = NVMDB::NVMGetCurrentTrxContext();
    festate->mTable = table;
    festate->mRowIndex = 0;
    festate->mNumAttrs = RelationGetNumberOfAttributes(rel);
    int len = BITMAP_GETLEN(festate->mNumAttrs);
    festate->mAttrsUsed = reinterpret_cast<uint8_t *>(palloc0(len));
    festate->mAttrsModified = reinterpret_cast<uint8_t *>(palloc0(len));
    needWholeRow = rel->trigdesc && rel->trigdesc->trig_insert_after_row;
    desc = RelationGetDescr(rel);

    if (NvmIsTxnInAbortState(festate->mCurrTxn)) {
        NVMDB::NvmRaiseAbortTxnError();
    }

    foreach (lc, baserel->baserestrictinfo) {
        RestrictInfo *ri = (RestrictInfo *)lfirst(lc);

        if (!needWholeRow)
            pull_varattnos((Node *)ri->clause, baserel->relid, &attrs);
    }

    if (needWholeRow) {
        for (int i = 0; i < desc->natts; i++) {
            if (!desc->attrs[i].attisdropped) {
                BITMAP_SET(festate->mAttrsUsed, (desc->attrs[i].attnum - 1));
            }
        }
    } else {
        /* Pull "var" clauses to build an appropriate target list */
        pull_varattnos((Node *)baserel->reltarget->exprs, baserel->relid, &attrs);
        if (attrs != NULL) {
            bool all = bms_is_member(-FirstLowInvalidHeapAttributeNumber, attrs);
            for (int i = 0; i < festate->mNumAttrs; i++) {
                if (all || bms_is_member(desc->attrs[i].attnum - FirstLowInvalidHeapAttributeNumber, attrs)) {
                    BITMAP_SET(festate->mAttrsUsed, (desc->attrs[i].attnum - 1));
                }
            }
        }
    }

    baserel->fdw_private = festate;
    baserel->rows = NVMDB_ESTIMATED_QUANTITY;
    baserel->tuples = NVMDB_ESTIMATED_QUANTITY;
    festate->mConst.mStartupCost = NVMDB_START_UP_COST;
    festate->mConst.mCost = baserel->rows * festate->mConst.mStartupCost;
    festate->mConstPara.mStartupCost = NVMDB_START_UP_COST;
    festate->mConstPara.mCost = baserel->rows * festate->mConst.mStartupCost;

    RelationClose(rel);

    return;
}

static void NVMGetForeignPaths(PlannerInfo *root, RelOptInfo *baserel, Oid foreigntableid)
{
    NVMFdwState *planstate = (NVMFdwState *)baserel->fdw_private;
    Path *fpReg = nullptr;
    List *usablePathkeys = NIL;
    Path *fpIx = nullptr;
    bool hasRegularPath = false;
    ListCell *lc = nullptr;
    Path *bestPath = nullptr;
    List *bestClause = nullptr;
    NVMDB::NvmMatchIndexArr matchArray;
    double ntuples;

    foreach (lc, baserel->baserestrictinfo) {
        RestrictInfo *ri = (RestrictInfo *)lfirst(lc);

        NVMDB::IsNVMExpr(baserel, planstate, ri->clause, nullptr, &matchArray);

        planstate->mLocalConds = lappend(planstate->mLocalConds, ri->clause);
    }

    if (NVMDB::NvmGetBestIndex(planstate, &matchArray, &(planstate->mConst)) != nullptr) {
        ntuples = planstate->mConst.mCost;
        ntuples = ntuples * clauselist_selectivity(root, bestClause, 0, JOIN_INNER, nullptr);
        ntuples = clamp_row_est(ntuples);
        baserel->rows = baserel->tuples = ntuples;
    }

    fpReg = (Path *)create_foreignscan_path(root, baserel, planstate->mConst.mStartupCost, planstate->mConst.mCost,
                                            usablePathkeys, nullptr, /* no outer rel either */
                                            nullptr,                 // private data will be assigned later
                                            0);

    foreach (lc, baserel->pathlist) {
        Path *path = (Path *)lfirst(lc);
        if (IsA(path, IndexPath) && path->param_info == nullptr) {
            hasRegularPath = true;
            break;
        }
    }
    if (!hasRegularPath)
        add_path(root, baserel, fpReg);
    set_cheapest(baserel);

    if (!IS_PGXC_COORDINATOR && list_length(baserel->cheapest_parameterized_paths) > 0) {
        foreach (lc, baserel->cheapest_parameterized_paths) {
            bestPath = (Path *)lfirst(lc);
            if (IsA(bestPath, IndexPath) && bestPath->param_info) {
                IndexPath *ip = (IndexPath *)bestPath;
                bestClause = ip->indexclauses;
                break;
            }
        }
        usablePathkeys = nullptr;
    }

    if (bestClause != nullptr) {
        matchArray.Clear();

        foreach (lc, bestClause) {
            RestrictInfo *ri = (RestrictInfo *)lfirst(lc);

            NVMDB::IsNVMExpr(baserel, planstate, ri->clause, nullptr, &matchArray);
        }

        if (NVMDB::NvmGetBestIndex(planstate, &matchArray, &(planstate->mConstPara)) != nullptr) {
            ntuples = planstate->mConstPara.mCost;
            ntuples = ntuples * clauselist_selectivity(root, bestClause, 0, JOIN_INNER, nullptr);
            ntuples = clamp_row_est(ntuples);
            baserel->rows = baserel->tuples = ntuples;
            fpIx = (Path *)create_foreignscan_path(root, baserel, planstate->mConstPara.mStartupCost,
                                                   planstate->mConstPara.mCost, usablePathkeys,
                                                   nullptr,  /* no outer rel either */
                                                   nullptr,  // private data will be assigned later
                                                   0);

            fpIx->param_info = bestPath->param_info;
        }
    }

    List *newPath = nullptr;
    List *origPath = baserel->pathlist;
    // disable index path
    foreach (lc, baserel->pathlist) {
        Path *path = (Path *)lfirst(lc);
        if (IsA(path, ForeignPath))
            newPath = lappend(newPath, path);
        else
            pfree(path);
    }

    list_free(origPath);
    baserel->pathlist = newPath;
    if (hasRegularPath)
        add_path(root, baserel, fpReg);
    if (fpIx != nullptr)
        add_path(root, baserel, fpIx);
    set_cheapest(baserel);

    return;
}

static ForeignScan *NVMGetForeignPlan(PlannerInfo *root, RelOptInfo *baserel, Oid foreigntableid,
                                      ForeignPath *best_path, List *tlist, List *scan_clauses, Plan *outer_plan)
{
    ListCell *lc = nullptr;
    ::Index scanRelid = baserel->relid;
    NVMFdwState *planstate = (NVMFdwState *)baserel->fdw_private;
    List *remote = nullptr;
    ForeignScan *scan = nullptr;

    if (best_path->path.param_info && planstate->mConstPara.mIndex != nullptr) {
        if (planstate->mConst.mIndex != nullptr) {
            list_free(planstate->mConst.mParentExprList);
            list_free(planstate->mConst.mExprList);
        }

        int ret = memcpy_s(&(planstate->mConst), sizeof(NVMFdwConstraint), &(planstate->mConstPara),
                           sizeof(NVMFdwConstraint));
        NVMDB::SecureRetCheck(ret);
        planstate->mConstPara.mIndex = nullptr;
        planstate->mConstPara.mMatchCount = 0;
        planstate->mConstPara.mParentExprList = nullptr;
        planstate->mConstPara.mExprList = nullptr;
    }

    if (planstate->mConst.mIndex != nullptr) {
        planstate->mNumExpr = list_length(planstate->mConst.mExprList);
        remote = list_concat(planstate->mConst.mExprList, planstate->mConst.mParentExprList);

        if (planstate->mConst.mParentExprList) {
            pfree(planstate->mConst.mParentExprList);
            planstate->mConst.mParentExprList = nullptr;
        }
    } else {
        planstate->mNumExpr = 0;
    }

    foreach (lc, scan_clauses) {
        RestrictInfo *ri = (RestrictInfo *)lfirst(lc);

        // add OR conditions which where not handled by previous functions
        if (ri->orclause != nullptr)
            planstate->mLocalConds = lappend(planstate->mLocalConds, ri->clause);
        else if (IsA(ri->clause, BoolExpr)) {
            BoolExpr *e = (BoolExpr *)ri->clause;

            if (e->boolop == NOT_EXPR)
                planstate->mLocalConds = lappend(planstate->mLocalConds, ri->clause);
        } else if (IsA(ri->clause, OpExpr)) {
            OpExpr *e = (OpExpr *)ri->clause;

            if (NVMDB::NVMIsNotEqualOper(e))
                planstate->mLocalConds = lappend(planstate->mLocalConds, ri->clause);
            else if (!list_member(remote, e))
                planstate->mLocalConds = lappend(planstate->mLocalConds, ri->clause);
        }
    }
    baserel->fdw_private = nullptr;
    List *quals = planstate->mLocalConds;
    scan = make_foreignscan(tlist, quals, scanRelid, remote, /* no expressions to evaluate */
                            (List *)NVMDB::NVMSerializeFdwState(planstate), NIL, NIL, NULL
#if PG_VERSION_NUM >= 90500
                            ,
                            nullptr, nullptr, nullptr
#endif
    );

    return scan;
}

List *NVMPlanForeignModify(PlannerInfo *root, ModifyTable *plan, ::Index resultRelation, int subplanIndex)
{
    NVMFdwState *fdwState = nullptr;
    RangeTblEntry *rte = planner_rt_fetch(resultRelation, root);
    Relation rel = heap_open(rte->relid, NoLock);
    TupleDesc desc = RelationGetDescr(rel);
    uint8_t attrsModify[BITMAP_GETLEN(desc->natts)];
    uint8_t *ptrAttrsModify = attrsModify;
    NVMDB::Transaction *currTxn = NVMDB::NVMGetCurrentTrxContext();
    NVMDB::Table *table = NVMDB::NvmGetTableByOidWrapper(RelationGetRelid(rel));

    if (NvmIsTxnInAbortState(currTxn)) {
        NVMDB::NvmRaiseAbortTxnError();
    }

    if ((int)resultRelation < root->simple_rel_array_size && root->simple_rel_array[resultRelation] != nullptr) {
        if (root->simple_rel_array[resultRelation]->fdw_private != nullptr) {
            fdwState = (NVMFdwState *)root->simple_rel_array[resultRelation]->fdw_private;
            ptrAttrsModify = fdwState->mAttrsUsed;
        }
    } else {
        fdwState = reinterpret_cast<NVMFdwState *>(palloc0(sizeof(NVMFdwState)));
        fdwState->mConst.mCost = std::numeric_limits<double>::max();
        fdwState->mConstPara.mCost = std::numeric_limits<double>::max();
        fdwState->mCmdOper = plan->operation;
        fdwState->mForeignTableId = rte->relid;
        fdwState->mNumAttrs = RelationGetNumberOfAttributes(rel);

        fdwState->mTable = table;

        int len = BITMAP_GETLEN(fdwState->mNumAttrs);
        fdwState->mAttrsUsed = reinterpret_cast<uint8_t *>(palloc0(len));
        fdwState->mAttrsModified = reinterpret_cast<uint8_t *>(palloc0(len));
    }

    switch (plan->operation) {
        case CMD_INSERT: {
            for (int i = 0; i < desc->natts; i++) {
                if (!desc->attrs[i].attisdropped) {
                    BITMAP_SET(fdwState->mAttrsUsed, (desc->attrs[i].attnum - 1));
                }
            }
            break;
        }
        case CMD_UPDATE: {
            errno_t erc = memset_s(attrsModify, BITMAP_GETLEN(desc->natts), 0, BITMAP_GETLEN(desc->natts));
            securec_check(erc, "\0", "\0");
            for (int i = 0; i < desc->natts; i++) {
                if (bms_is_member(desc->attrs[i].attnum - FirstLowInvalidHeapAttributeNumber, rte->updatedCols)) {
                    BITMAP_SET(ptrAttrsModify, (desc->attrs[i].attnum - 1));
                }
            }
            break;
        }
        case CMD_DELETE: {
            if (list_length(plan->returningLists) > 0) {
                errno_t erc = memset_s(attrsModify, BITMAP_GETLEN(desc->natts), 0, BITMAP_GETLEN(desc->natts));
                securec_check(erc, "\0", "\0");
                for (int i = 0; i < desc->natts; i++) {
                    if (!desc->attrs[i].attisdropped) {
                        BITMAP_SET(ptrAttrsModify, (desc->attrs[i].attnum - 1));
                    }
                }
            }
            break;
        }
        default:
            break;
    }

    heap_close(rel, NoLock);

    return ((fdwState == nullptr) ? (List *)NVMDB::NVMBitmapSerialize(nullptr, attrsModify, BITMAP_GETLEN(desc->natts))
                                  : (List *)NVMDB::NVMSerializeFdwState(fdwState));

    return nullptr;
}

static constexpr int NVMDB_CURRENT_INDENT_LEVEL = 2;
static void NVMExplainForeignScan(ForeignScanState *node, ExplainState *es)
{
    NVMFdwState *festate = nullptr;
    bool isLocal = false;
    ForeignScan *fscan = (ForeignScan *)node->ss.ps.plan;

    if (node->fdw_state != nullptr) {
        festate = (NVMFdwState *)node->fdw_state;
    } else {
        festate = NVMDB::NVMInitializeFdwState(fscan->fdw_private, &fscan->fdw_exprs,
                                               RelationGetRelid(node->ss.ss_currentRelation));
        isLocal = true;
    }

    if (festate->mConst.mIndex != nullptr) {
        Node *qual = nullptr;
        List *context = nullptr;
        char *exprstr = nullptr;

        // details for index
        appendStringInfoSpaces(es->str, es->indent);

        qual = (Node *)make_ands_explicit(festate->mConst.mParentExprList);

        /* Set up deparsing context */
        context = deparse_context_for_planstate((Node *)&(node->ss.ps), NULL, es->rtable);

        /* Deparse the expression */
        exprstr = deparse_expression(qual, context, true, false);

        if (t_thrd.explain_cxt.explain_perf_mode != EXPLAIN_NORMAL && es->planinfo->m_detailInfo) {
            es->planinfo->m_detailInfo->set_plan_name<true, true>();
            appendStringInfo(es->planinfo->m_detailInfo->info_str, "%s: %s\n", "Index Cond", exprstr);
        }

        /* And add to es->str */
        ExplainPropertyText("Index Cond", exprstr, es);
        es->indent += NVMDB_CURRENT_INDENT_LEVEL;
    }

    if (isLocal) {
        NVMDB::NVMStateFree(festate);
    }
}

static void NVMBeginForeignScan(ForeignScanState *node, int eflags)
{
    ListCell *t = NULL;
    NVMFdwState *festate = nullptr;
    ForeignScan *fscan = (ForeignScan *)node->ss.ps.plan;
    NVMDB::Table *table = nullptr;
    Oid tableId = RelationGetRelid(node->ss.ss_currentRelation);

    node->ss.is_scan_end = false;

    table = NVMDB::NvmGetTableByOidWrapper(tableId);
    festate = NVMDB::NVMInitializeFdwState(fscan->fdw_private, &fscan->fdw_exprs, tableId);
    festate->mCurrTxn = NVMDB::NVMGetCurrentTrxContext();
    festate->mTable = table;
    festate->mRowIndex = 0;
    node->fdw_state = festate;

    if (NvmIsTxnInAbortState(festate->mCurrTxn)) {
        NVMDB::NvmRaiseAbortTxnError();
    }

    if (node->ss.ps.state->es_result_relation_info &&
        (RelationGetRelid(node->ss.ps.state->es_result_relation_info->ri_RelationDesc) ==
         RelationGetRelid(node->ss.ss_currentRelation)))
        node->ss.ps.state->es_result_relation_info->ri_FdwState = festate;

    foreach (t, node->ss.ps.plan->targetlist) {
        TargetEntry *tle = (TargetEntry *)lfirst(t);
        Var *v = (Var *)tle->expr;
        if (v->varattno == SelfItemPointerAttributeNumber && v->vartype == TIDOID) {
            festate->mCtidNum = tle->resno;
            break;
        }
    }

    return;
}

static TupleTableSlot *NVMIterateForeignScan(ForeignScanState *node)
{
    if (node->ss.is_scan_end) {
        return nullptr;
    }

    NVMFdwState *festate = (NVMFdwState *)node->fdw_state;
    TupleTableSlot *slot = node->ss.ss_ScanTupleSlot;
    NVMDB::Table *table = festate->mTable;
    NVMDB::HAM_STATUS status;
    bool found = false;
    TupleTableSlot *result = nullptr;

    char *tupleAddr = (char *)palloc(table->GetRowLen());
    NVMDB::RAMTuple tuple(table->GetColDesc(), table->GetRowLen(), tupleAddr, nullptr);

    NVMDB::NvmFdwIter *iter = NVMDB::NvmGetIter(node, festate);
    while (iter->Valid()) {
        status = NVMDB::HeapRead(festate->mCurrTxn, table, iter->GetRowId(), &tuple);
        if (status == NVMDB::HAM_SUCCESS) {
            found = true;
            break;
        }
        iter->Next();
    }

    if (found) {
        (void)ExecClearTuple(slot);
        NVMDB::NVMFillSlotByTuple(slot, table, &tuple);
        ExecStoreVirtualTuple(slot);
        result = slot;

        if (festate->mCtidNum > 0) {  // update or delete
            NVMDB::NvmRowId rowId;
            rowId.m_rowId = iter->GetRowId();
            HeapTuple resultTup = ExecFetchSlotTuple(slot);
            errno_t ret = memcpy_s(&resultTup->t_self, sizeof(rowId), &rowId, sizeof(rowId));
            NVMDB::SecureRetCheck(ret);
            HeapTupleSetXmin(resultTup, InvalidTransactionId);
            HeapTupleSetXmax(resultTup, InvalidTransactionId);
            HeapTupleHeaderSetCmin(resultTup->t_data, InvalidTransactionId);
        }
        iter->Next();
    } else {
        pfree(tupleAddr);
    }

    if (!iter->Valid()) {
        node->ss.is_scan_end = true;
    }

    return result;
}

static void NVMReScanForeignScan(ForeignScanState *node)
{
    NVMFdwState *festate = (NVMFdwState *)node->fdw_state;
    node->ss.is_scan_end = false;

    if (NvmIsTxnInAbortState(festate->mCurrTxn)) {
        NVMDB::NvmRaiseAbortTxnError();
    }

    if (festate->mIter != nullptr) {
        delete festate->mIter;
        festate->mIter = nullptr;
    }

    NVMDB::NvmGetIter(node, festate);

    return;
}

static void NVMEndForeignScan(ForeignScanState *node)
{
    NVMFdwState *fdwState = (NVMFdwState *)node->fdw_state;
    if (fdwState->mAllocInScan) {
        NVMDB::NVMStateFree(fdwState);
        node->fdw_state = NULL;
    }
}

static bool NVMAnalyzeForeignTable(Relation relation, AcquireSampleRowsFunc *func, BlockNumber *totalpages,
                                   void *additionalData, bool estimateTableRowNum)
{
    return true;
}

static int NVMAcquireSampleRowsFunc(Relation relation, int elevel, HeapTuple *rows, int targrows, double *totalrows,
                                    double *totaldeadrows, void *additionalData, bool estimateTableRowNum)
{
    return 0;
}

static void NVMValidateTableDef(Node *obj)
{
    ::TransactionId tid = GetCurrentTransactionId();
    if (obj == nullptr) {
        return;
    }

    switch (nodeTag(obj)) {
        case T_CreateForeignTableStmt: {
            NVMDB::CreateTable((CreateForeignTableStmt *)obj, tid);
            break;
        }
        case T_IndexStmt: {
            NVMDB::CreateIndex((IndexStmt *)obj, tid);
            break;
        }
        case T_DropForeignStmt: {
            DropForeignStmt *stmt = (DropForeignStmt *)obj;
            switch (stmt->relkind) {
                case RELKIND_INDEX:
                    NVMDB::DropIndex(stmt, tid);
                    break;
                case RELKIND_RELATION:
                    NVMDB::DropTable(stmt, tid);
                    break;
                default:
                    ereport(ERROR, (errcode(ERRCODE_FDW_OPERATION_NOT_SUPPORTED), errmodule(MOD_NVM),
                                    errmsg("The operation is not supported on nvmdb right now.")));
                    break;
            }
            break;
        }
        default:
            ereport(ERROR, (errcode(ERRCODE_FDW_OPERATION_NOT_SUPPORTED), errmodule(MOD_NVM),
                            errmsg("The operation is not supported on nvmdb right now.")));
    }
}

static void NVMBeginForeignModify(ModifyTableState *mtstate, ResultRelInfo *resultRelInfo, List *fdwPrivate,
                                  int subplanIndex, int eflags)
{
    NVMFdwState *festate = nullptr;

    if (fdwPrivate != nullptr && resultRelInfo->ri_FdwState == nullptr) {
        festate = NVMDB::NVMInitializeFdwState(fdwPrivate, nullptr, RelationGetRelid(resultRelInfo->ri_RelationDesc));
        festate->mAllocInScan = false;
        festate->mCurrTxn = NVMDB::NVMGetCurrentTrxContext();
        festate->mTable = NVMDB::NvmGetTableByOidWrapper(RelationGetRelid(resultRelInfo->ri_RelationDesc));
        resultRelInfo->ri_FdwState = festate;
    } else {
        festate = (NVMFdwState *)resultRelInfo->ri_FdwState;
        int len = BITMAP_GETLEN(festate->mNumAttrs);
        if (fdwPrivate != nullptr) {
            ListCell *cell = list_head(fdwPrivate);
            NVMDB::NVMBitmapDeSerialize(festate->mAttrsModified, len, &cell);

            for (int i = 0; i < festate->mNumAttrs; i++) {
                if (BITMAP_GET(festate->mAttrsModified, i)) {
                    BITMAP_SET(festate->mAttrsUsed, i);
                }
            }
        } else {
            errno_t erc = memset_s(festate->mAttrsUsed, len, 0xff, len);
            securec_check(erc, "\0", "\0");
            erc = memset_s(festate->mAttrsModified, len, 0xff, len);
            securec_check(erc, "\0", "\0");
        }
    }

    if (NvmIsTxnInAbortState(festate->mCurrTxn)) {
        NVMDB::NvmRaiseAbortTxnError();
    }

    // Update FDW operation
    festate->mCtidNum =
        ExecFindJunkAttributeInTlist(mtstate->mt_plans[subplanIndex]->plan->targetlist, NVM_REC_TID_NAME);
    festate->mCmdOper = mtstate->operation;
    return;
}

static inline TupleTableSlot *NVMExecForeignInsert(EState *estate, ResultRelInfo *resultRelInfo, TupleTableSlot *slot,
                                                   TupleTableSlot *planSlot)
{
    return NVMDB::NVMExecForeignInsertImplement(estate, resultRelInfo, slot, planSlot);
}

static TupleTableSlot *NVMExecForeignUpdate(EState *estate, ResultRelInfo *resultRelInfo, TupleTableSlot *slot,
                                            TupleTableSlot *planSlot)
{
    NVMFdwState *fdwState = (NVMFdwState *)resultRelInfo->ri_FdwState;
    AttrNumber num = fdwState->mCtidNum - 1;
    TupleDesc tupdesc = slot->tts_tupleDescriptor;
    NVMDB::HAM_STATUS result;
    NVMDB::Table *table = fdwState->mTable;
    const NVMDB::ColumnDesc *desc = table->GetColDesc();
    uint32 indexCount = table->GetIndexCount();
    bool indexColChange[indexCount] = {};
    NVMDB::NvmRowId rowId;
    uint64 i = 0;
    uint64 j = 1;

    Assert(num == table->GetColCount());

    if (fdwState->mCtidNum != 0 && planSlot->tts_nvalid >= fdwState->mCtidNum && !planSlot->tts_isnull[num]) {
        int ret = memcpy_s(&rowId, sizeof(rowId), reinterpret_cast<ItemPointerData *>(planSlot->tts_values[num]),
                           sizeof(rowId));
        NVMDB::SecureRetCheck(ret);
    } else {
        NVMAssert(false);
        return nullptr;
    }

    NVMDB::RAMTuple tuple(table->GetColDesc(), table->GetRowLen());
    result = NVMDB::HeapRead(fdwState->mCurrTxn, fdwState->mTable, rowId.m_rowId, &tuple);
    if (result != NVMDB::HAM_SUCCESS) {
        NVMAssert(false);
        return nullptr;
    }

    NVMDB::RAMTuple tupleOrg(table->GetColDesc(), table->GetRowLen());
    tupleOrg.CopyRow(&tuple);

    for (; i < num; i++, j++) {
        if (BITMAP_GET(fdwState->mAttrsModified, i)) {
            bool isnull = false;
            Datum value = heap_slot_getattr(planSlot, j, &isnull);
            if (!isnull) {
                NVMDB::NVMColUpdateData(tuple, i, value, tupdesc->attrs[i].atttypid, desc[i].m_colLen);
                tuple.SetNull(i, false);
            } else {
                tuple.SetNull(i);
            }
        }
    }

    result = NVMDB::HeapUpdate(fdwState->mCurrTxn, fdwState->mTable, rowId.m_rowId, &tuple);
    if (result != NVMDB::HAM_SUCCESS) {
        ereport(ERROR, (errcode(ERRCODE_T_R_SERIALIZATION_FAILURE), errmsg("NVM Update fail(%d)!", result)));
    }

    for (i = 0; i < num; i++) {
        if (BITMAP_GET(fdwState->mAttrsModified, i)) {
            for (uint32 k = 0; k < indexCount; k++) {
                NVMDB::NVMIndex *index = table->GetIndex(k);
                if (index != nullptr && index->IsFieldPresent(i)) {
                    indexColChange[k] = true;
                }
            }
        }
    }

    for (uint32 k = 0; k < indexCount; k++) {
        if (indexColChange[k]) {
            NVMDB::NVMIndex *index = table->GetIndex(k);
            NVMDB::NVMInsertTuple2Index(fdwState->mCurrTxn, table, index, &tuple, rowId.m_rowId);
            NVMDB::NVMDeleteTupleFromIndex(fdwState->mCurrTxn, table, index, &tupleOrg, rowId.m_rowId);
        }
    }

    if (resultRelInfo->ri_projectReturning) {
        return planSlot;
    } else {
        estate->es_processed++;
        return nullptr;
    }
}

static TupleTableSlot *NVMExecForeignDelete(EState *estate, ResultRelInfo *resultRelInfo, TupleTableSlot *slot,
                                            TupleTableSlot *planSlot)
{
    NVMFdwState *fdwState = (NVMFdwState *)resultRelInfo->ri_FdwState;
    AttrNumber num = fdwState->mCtidNum - 1;
    NVMDB::Table *table = fdwState->mTable;
    NVMDB::HAM_STATUS result;
    NVMDB::NvmRowId rowId;

    if (fdwState->mCtidNum != 0 && planSlot->tts_nvalid >= fdwState->mCtidNum && !planSlot->tts_isnull[num]) {
        int ret = memcpy_s(&rowId, sizeof(rowId), reinterpret_cast<ItemPointerData *>(planSlot->tts_values[num]),
                           sizeof(rowId));
        NVMDB::SecureRetCheck(ret);
    } else {
        NVMAssert(false);
        return nullptr;
    }

    NVMDB::RAMTuple tuple(table->GetColDesc(), table->GetRowLen());
    result = NVMDB::HeapRead(fdwState->mCurrTxn, table, rowId.m_rowId, &tuple);
    if (result != NVMDB::HAM_SUCCESS) {
        NVMAssert(false);
        return nullptr;
    }

    result = NVMDB::HeapDelete(fdwState->mCurrTxn, fdwState->mTable, rowId.m_rowId);
    if (result != NVMDB::HAM_SUCCESS) {
        ereport(ERROR, (errcode(ERRCODE_T_R_SERIALIZATION_FAILURE), errmsg("NVM Delete fail(%d)!", result)));
    }

    NVMDB::NVMDeleteTupleFromAllIndex(fdwState->mCurrTxn, fdwState->mTable, &tuple, rowId.m_rowId);

    if (resultRelInfo->ri_projectReturning) {
        return planSlot;
    } else {
        estate->es_processed++;
        return nullptr;
    }
}

static void NVMEndForeignModify(EState *estate, ResultRelInfo *resultRelInfo)
{
    NVMFdwState *fdwState = (NVMFdwState *)resultRelInfo->ri_FdwState;

    if (!fdwState->mAllocInScan) {
        NVMDB::NVMStateFree(fdwState);
        resultRelInfo->ri_FdwState = nullptr;
    }
}

static int NVMIsForeignRelationUpdatable(Relation rel)
{
    return (1 << CMD_UPDATE) | (1 << CMD_INSERT) | (1 << CMD_DELETE);
}

static int NVMGetFdwType()
{
    return NVM_ORC;
}

static void NVMTruncateForeignTable(TruncateStmt *stmt, Relation rel)
{
    return;
}

static void NVMVacuumForeignTable(VacuumStmt *stmt, Relation rel)
{
    return;
}

static uint64_t NVMGetForeignRelationMemSize(Oid reloid, Oid ixoid)
{
    return 0;
}

static void NVMNotifyForeignConfigChange()
{
    return;
}

static void NVMXactCallback(XactEvent event, void *arg)
{
    NVMDB::Transaction *trans = NVMDB::NVMGetCurrentTrxContext();

    if (event == XACT_EVENT_START) {
        trans->Begin();
    } else if (event == XACT_EVENT_COMMIT) {
        trans->Commit();
    } else if (event == XACT_EVENT_ABORT) {
        trans->Abort();
    }

    return;
}

static void NVMSubxactCallback(SubXactEvent event, SubTransactionId mySubid, SubTransactionId parentSubid, void *arg)
{
    return;
}

Datum nvm_fdw_handler(PG_FUNCTION_ARGS)
{
    FdwRoutine *fdwroutine = makeNode(FdwRoutine);
    fdwroutine->AddForeignUpdateTargets = NVMAddForeignUpdateTargets;
    fdwroutine->GetForeignRelSize = NVMGetForeignRelSize;
    fdwroutine->GetForeignPaths = NVMGetForeignPaths;
    fdwroutine->GetForeignPlan = NVMGetForeignPlan;
    fdwroutine->PlanForeignModify = NVMPlanForeignModify;
    fdwroutine->ExplainForeignScan = NVMExplainForeignScan;
    fdwroutine->BeginForeignScan = NVMBeginForeignScan;
    fdwroutine->IterateForeignScan = NVMIterateForeignScan;
    fdwroutine->ReScanForeignScan = NVMReScanForeignScan;
    fdwroutine->EndForeignScan = NVMEndForeignScan;
    fdwroutine->AnalyzeForeignTable = NVMAnalyzeForeignTable;
    fdwroutine->AcquireSampleRows = NVMAcquireSampleRowsFunc;
    fdwroutine->ValidateTableDef = NVMValidateTableDef;
    fdwroutine->PartitionTblProcess = NULL;
    fdwroutine->BuildRuntimePredicate = NULL;
    fdwroutine->BeginForeignModify = NVMBeginForeignModify;
    fdwroutine->ExecForeignInsert = NVMExecForeignInsert;
    fdwroutine->ExecForeignUpdate = NVMExecForeignUpdate;
    fdwroutine->ExecForeignDelete = NVMExecForeignDelete;
    fdwroutine->EndForeignModify = NVMEndForeignModify;
    fdwroutine->IsForeignRelUpdatable = NVMIsForeignRelationUpdatable;
    fdwroutine->GetFdwType = NVMGetFdwType;
    fdwroutine->TruncateForeignTable = NVMTruncateForeignTable;
    fdwroutine->VacuumForeignTable = NVMVacuumForeignTable;
    fdwroutine->GetForeignRelationMemSize = NVMGetForeignRelationMemSize;
    fdwroutine->GetForeignMemSize = NULL;
    fdwroutine->GetForeignSessionMemSize = NULL;
    fdwroutine->NotifyForeignConfigChange = NVMNotifyForeignConfigChange;

    NVMDB::NVMGetCurrentTrxContext();

    if (!u_sess->nvm_cxt.init_flag) {
        RegisterXactCallback(NVMXactCallback, NULL);
        RegisterSubXactCallback(NVMSubxactCallback, NULL);
        u_sess->nvm_cxt.init_flag = true;
    }

    PG_RETURN_POINTER(fdwroutine);
}

void InitNvm()
{
    int ret;
    struct stat st;
    std::string nvmDirPath = g_instance.attr.attr_common.nvm_directory;
    std::string dataPath = g_instance.attr.attr_common.data_directory;
    std::string singlePath;
    bool needInit = false;

    if (!is_absolute_path(nvmDirPath)) {
        // for one path, default is pg_nvm
        nvmDirPath = dataPath + "/" + nvmDirPath;
        if (stat(nvmDirPath.c_str(), &st) != 0) {
            needInit = true;
        }
    } else {
        // for absolute path, set by user. Count must less than or equal to NVMDB_MAX_GROUP
        for (int i = 0, j = 0; j < nvmDirPath.size(); ++j) {
            if (nvmDirPath[j] == ';' || j == nvmDirPath.size() - 1) {
                if (j == nvmDirPath.size() - 1) {
                    j++;
                }
                assert(j > i);
                singlePath = nvmDirPath.substr(i, j - i);
                if (stat(singlePath.c_str(), &st) != 0) {
                    needInit = true;
                }
                i = j + 1;
            }
        }
    }

    if (needInit) {
        ereport(INFO, (errmsg("NVMDB begin Init!")));
        NVMDB::InitDB(nvmDirPath.c_str());
        ereport(INFO, (errmsg("NVMDB end Init!")));
    } else {
        ereport(INFO, (errmsg("NVMDB begin BootStrap!")));
        NVMDB::BootStrap(nvmDirPath.c_str());
        ereport(INFO, (errmsg("NVMDB end BootStrap!")));
    }
}

void UinitNvm()
{
    NVMDB::ExitDBProcess();
}

void UinitNvmSession()
{
    if (u_sess->nvm_cxt.trx != nullptr) {
        delete u_sess->nvm_cxt.trx;
        u_sess->nvm_cxt.trx = NULL;
    }
}

void NVMDBOnExist(int code, Datum arg)
{
    assert(t_thrd.nvmdb_init == true);
    UinitNvmSession();
    NVMDB::DestroyThreadLocalVariables();
    NVMDB::NvmThreadlocalTableMapClear();
    t_thrd.nvmdb_init = false;
}

void InitNvmThread()
{
    if (!t_thrd.nvmdb_init) {
        NVMDB::InitThreadLocalVariables();
        on_proc_exit(NVMDBOnExist, 0);
        t_thrd.nvmdb_init = true;
    }
}
