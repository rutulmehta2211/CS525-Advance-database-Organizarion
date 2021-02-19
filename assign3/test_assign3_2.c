#include <stdlib.h>
#include "dberror.h"
#include "expr.h"
#include "record_mgr.h"
#include "tables.h"
#include "test_helper.h"

#define ASSERT_EQUALS_RECORDS(_l,_r, sch, m)			\
  do {									\
    Record *_lR = _l;                                                   \
    Record *_rR = _r;                                                   \
    ASSERT_TRUE(memcmp(_lR->data,_rR->data,getRecordSize(sch)) == 0, m); \
    int i;								\
    for(i = 0; i < sch->numAttr; i++)				\
      {									\
        Value *lVal, *rVal;                                             \
		char *lSer, *rSer; \
        getAttr(_lR, sch, i, &lVal);                                  \
        getAttr(_rR, sch, i, &rVal);                                  \
		lSer = serializeValue(lVal); \
		rSer = serializeValue(rVal); \
        ASSERT_EQUALS_STRING(lSer, rSer, "Same attribute");	\
		free(lVal); \
		free(rVal); \
		free(lSer); \
		free(rSer); \
      }									\
  } while(0)

#define ASSERT_EQUALS_RECORD_IN(_l,_r, rSize, sch, m)		\
  do {									\
    int j;								\
    boolean found = false;						\
    for(j = 0; j < rSize; j++)						\
      if (memcmp(_l->data,_r[j]->data,getRecordSize(sch)) == 0)	\
	found = true;							\
    ASSERT_TRUE(0, m);						\
  } while(0)

#define OP_TRUE(le, ri, op, m)		\
  do {							\
    Value *rlt = (Value *) malloc(sizeof(Value));	\
    op(le, ri, rlt);				\
    bool b = rlt->v.boolV;				\
    free(rlt);					\
    ASSERT_TRUE(b,m);				\
   } while (0)

// test methods
static void UpdateScanTest(void);

// ----- structure defined ------
typedef struct StructTestRecord {
    int one;
    char *two;
    int three;
} StructTestRecord;

Schema *schemaTest (void);
Record *fromRecordTest (StructTestRecord structTestRecord, Schema *schema);
Record *recordTest(int one, char *two, int three, Schema *schema);

// ----name of the test used ----
char *testName;

// main method
int
main (void)
{
    testName = "";
    UpdateScanTest();
    return 0;
}

void UpdateScanTest(void)
{
    int count, rc, insertsNum = 10;
    RM_TableData *tab = (RM_TableData *) malloc(sizeof(RM_TableData));
    RID *ridss = (RID *) malloc(sizeof(RID) * insertsNum);
    Schema *sch = schemaTest();
    Record *updateRecord, *r;
    RM_ScanHandle *sc = (RM_ScanHandle *) malloc(sizeof(RM_ScanHandle));
    Expr *sel, *le, *ri, *fst, *se;
    testName = "Test to update scan";

    StructTestRecord inserts[] = {
            {1, "abcd", 3},
            {2, "efgh", 2},
            {3, "ijkl", 1},
            {4, "mnop", 3},
            {5, "qrst", 5},
            {6, "uvwx", 1},
            {7, "yzab", 3},
            {8, "cdef", 3},
            {9, "ghij", 2},
            {10, "klmn", 5},
    };
    bool foundScan[] = {
            FALSE,
            FALSE,
            FALSE,
            FALSE,
            FALSE,
            FALSE,
            FALSE,
            FALSE,
            FALSE,
            FALSE
    };
    StructTestRecord updates[] = {
            {8888, "ghij", 8}
    };
    StructTestRecord updateRec[] = {
            {1, "abcd", 3},
            {2, "efgh", 2},
            {3, "ijkl", 1},
            {4, "mnop", 3},
            {8888, "ghij", 8},
           {6, "uvwx", 1},
            {7, "yzab", 3},
            {8, "cdef", 3},
            {9, "ghij", 2},
            {8888, "ghij", 8},
    };

    TEST_CHECK(initRecordManager(NULL));
    TEST_CHECK(createTable("test_table_r",sch));
    TEST_CHECK(openTable(tab, "test_table_r"));

    // Insert rows into table
    count=0;
    while(count < insertsNum)
    {
        r = fromRecordTest(inserts[count], sch);
        TEST_CHECK(insertRecord(tab,r));
        ridss[count] = r->id;
        count++;
    }

    TEST_CHECK(closeTable(tab));
    TEST_CHECK(openTable(tab, "test_table_r"));

    MAKE_CONS(le, stringToValue("i2"));
    MAKE_ATTRREF(ri, 0);
    MAKE_BINOP_EXPR(sel, le, ri, OP_COMP_EQUAL);
    createRecord(&r, sch);
    TEST_CHECK(startScan(tab, sc, sel));
    while((rc = next(sc, r)) == RC_OK)
    {
        ASSERT_EQUALS_RECORDS(fromRecordTest(inserts[1],sch), r, sch, "comparing the records");
    }
    if (rc != RC_RM_NO_MORE_TUPLES)
        TEST_CHECK(rc);
    TEST_CHECK(closeScan(sc));

    MAKE_CONS(le, stringToValue("suvwx"));
    MAKE_ATTRREF(ri, 1);
    MAKE_BINOP_EXPR(sel, le, ri, OP_COMP_EQUAL);
    createRecord(&r, sch);
    TEST_CHECK(startScan(tab, sc, sel));
    while((rc = next(sc, r)) == RC_OK)
    {
        ASSERT_EQUALS_RECORDS(fromRecordTest(inserts[5],sch), r, sch, "comparing the records");
        serializeRecord(r, sch);
    }
    if (rc != RC_RM_NO_MORE_TUPLES)
        TEST_CHECK(rc);
    TEST_CHECK(closeScan(sc));

    //---Choose only those records that evaulate to false--------
    MAKE_CONS(le, stringToValue("i4"));
    MAKE_ATTRREF(ri, 2);
    MAKE_BINOP_EXPR(fst, ri, le, OP_COMP_SMALLER);
    MAKE_UNOP_EXPR(se, fst, OP_BOOL_NOT);
    TEST_CHECK(startScan(tab, sc, se));
    updateRecord = fromRecordTest(updates[0],sch);
    updateScan(tab,r,updateRecord,sc);
    TEST_CHECK(closeTable(tab));
    
    TEST_CHECK(shutdownRecordManager());

    TEST_CHECK(initRecordManager(NULL));
    
    TEST_CHECK(openTable(tab, "test_table_r"));

    MAKE_CONS(le, stringToValue("i7"));
    MAKE_ATTRREF(ri, 2);
    MAKE_BINOP_EXPR(fst, ri, le, OP_COMP_SMALLER);
    MAKE_UNOP_EXPR(se, fst, OP_BOOL_NOT);
    TEST_CHECK(startScan(tab, sc, se));

    while((rc = next(sc, r)) == RC_OK)
    {
        serializeRecord(r, sch);
        count=0;
        while(count < insertsNum)
        {
            if (memcmp(fromRecordTest(updateRec[count],sch)->data,r->data,getRecordSize(sch)) == 0)
                foundScan[count] = TRUE;
            count++;
        }
    }
    if (rc != RC_RM_NO_MORE_TUPLES)
        TEST_CHECK(rc);
    TEST_CHECK(closeScan(sc));

    ASSERT_TRUE(!foundScan[0], "Is not >7");
    ASSERT_TRUE(foundScan[4], ">7");
    ASSERT_TRUE(foundScan[9], ">7");

    // clean up
    TEST_CHECK(closeTable(tab));
    TEST_CHECK(deleteTable("test_table_r"));
    TEST_CHECK(shutdownRecordManager());

    freeRecord(r);
    free(tab);
    free(sc);
    freeExpr(sel);
    TEST_DONE();
}

Schema *
schemaTest (void)
{
    Schema *rlt;

    char **cpNames = (char **) malloc(sizeof(char*) * 3);
    char *names[] = { "a", "b", "c" };

    DataType *cpDt = (DataType *) malloc(sizeof(DataType) * 3);
    DataType dt[] = { DT_INT, DT_STRING, DT_INT };
    
    int *cpSizes = (int *) malloc(sizeof(int) * 3);
    int sizes[] = { 0, 4, 0 };
    
    int *cpKeys = (int *) malloc(sizeof(int));
    int key[] = {0};

    int i=0;
    while(i < 3)
    {
        cpNames[i] = (char *) malloc(2);
        strcpy(cpNames[i], names[i]);
        i++;
    }
    memcpy(cpDt, dt, sizeof(DataType) * 3);
    memcpy(cpSizes, sizes, sizeof(int) * 3);
    memcpy(cpKeys, key, sizeof(int));

    rlt = createSchema(3, cpNames, cpDt, cpSizes, 1, cpKeys);

    return rlt;
}

Record *
fromRecordTest (StructTestRecord structTestRecord, Schema *schema)
{
    return recordTest(structTestRecord.one, structTestRecord.two, structTestRecord.three, schema);
}

Record *
recordTest(int one, char *two, int three, Schema *schema)
{
    Value *v;
    Record *rlt;

    TEST_CHECK(createRecord(&rlt, schema));

    MAKE_VALUE(v, DT_INT, one);
    TEST_CHECK(setAttr(rlt, schema, 0, v));
    freeVal(v);

    MAKE_STRING_VALUE(v, two);
    TEST_CHECK(setAttr(rlt, schema, 1, v));
    freeVal(v);

    MAKE_VALUE(v, DT_INT, three);
    TEST_CHECK(setAttr(rlt, schema, 2, v));
    freeVal(v);

    return rlt;
}
