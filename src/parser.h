#ifndef __PARSER__
#define __PARSER__ 
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <ctype.h>
#include <inttypes.h>
#include "journal.h"
#include "extendibleHashing.h"



//---------------------------------------------------------------------------
typedef struct MessageHead {
   /// Total message length, excluding this head
   uint32_t messageLen;
   /// The message type
   Type_t type;
} MessageHead_t;

//---------------------------------------------------------------------------
typedef struct DefineSchema {
   /// Number of relations
   uint32_t relationCount;
   /// Column counts per relation, one count per relation. The first column is always the primary key
   uint32_t columnCounts[];
} DefineSchema_t;

//---------------------------------------------------------------------------
typedef struct Transaction {
   /// The transaction id. Monotonic increasing
   uint64_t transactionId;
   /// The operation counts
   uint32_t deleteCount,insertCount;
   /// A sequence of transaction operations. Deletes first, total deleteCount+insertCount operations
   char operations[];
} Transaction_t;

//---------------------------------------------------------------------------
typedef struct TransactionOperationDelete {
   /// The affected relation
   uint32_t relationId;
   /// The row count
   uint32_t rowCount;
   /// The deleted values, rowCount primary keyss
   uint64_t keys[];
} TransactionOperationDelete_t;

//---------------------------------------------------------------------------
typedef struct TransactionOperationInsert {
   /// The affected relation
   uint32_t relationId;
   /// The row count
   uint32_t rowCount;
   /// The inserted values, rowCount*relation[relationId].columnCount values
   uint64_t values[];
} TransactionOperationInsert_t;

//---------------------------------------------------------------------------
typedef struct ValidationQueries {
   /// The validation id. Monotonic increasing
   uint64_t validationId;
   /// The transaction range
   uint64_t from,to;
   /// The query count
   uint32_t queryCount;
   /// The queries
   char queries[];
} ValidationQueries_t;

//---------------------------------------------------------------------------

//---------------------------------------------------------------------------
typedef struct Query {

   /// The relation
   uint32_t relationId;
   /// The number of bound columns
   uint32_t columnCount;
   /// The bindings
   Column_t columns[];
} Query_t;

//---------------------------------------------------------------------------
typedef struct Flush {
   /// All validations to this id (including) must be answered
   uint64_t validationId;
} Flush_t;

//---------------------------------------------------------------------------
typedef struct Forget {
   /// Transactions older than that (including) will not be tested for
   uint64_t transactionId;
} Forget_t;

typedef struct SingleQuery {
   /// The relation
   uint32_t relationId;
   /// The number of bound columns
   uint32_t columnCount;
   /// The bindings
   Column_t** columns;
} SingleQuery_t;


typedef struct ValQuery {
	/// The validation id. Monotonic increasing
	uint64_t validationId;
	/// The transaction range
	uint64_t from,to;
	/// The query count
	uint32_t queryCount;
	/// The queries
	SingleQuery_t** queries;
} ValQuery_t;



typedef struct ValidationList {
	ValQuery_t** validation_array;
	uint64_t num_of_validations;
	uint64_t capacity;
} ValidationList_t;



// void skipWhiteSpaces(char**);
// char* getFirstToken(char**);
int validationListInsert(ValidationList_t* , ValQuery_t*);

void validationListPrint(ValidationList_t*, Journal_t**);

ValidationList_t* validationListCreate();

void validationListDestroy(ValidationList_t*);

void destroyValQuery(ValQuery_t*);

void destroySingleQuery(SingleQuery_t*);

Journal_t** processDefineSchema(DefineSchema_t *, int*);

void processTransaction(Transaction_t *, Journal_t**);

void processValidationQueries(ValidationQueries_t *, Journal_t**, ValidationList_t*);

void processFlush(Flush_t *, Journal_t**, ValidationList_t*);

Boolean_t checkValidation(Journal_t**, ValQuery_t*);

Boolean_t checkSingleQuery(Journal_t**, SingleQuery_t*, uint64_t, uint64_t);

Boolean_t checkColumn(Journal_t*, Column_t*, uint64_t, uint64_t);


void processForget(Forget_t *, Journal_t**);


Forget_t* forgetParser(char **);

void destroySchema(Journal_t**, int);

void printValidation(ValQuery_t*, Journal_t**);

int cmp_col(const void *, const void *);

// void copyValidation(ValidationQueries_t*, ValidationQueries_t*);

#endif
