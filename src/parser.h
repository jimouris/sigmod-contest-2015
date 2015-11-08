#ifndef __PARSER__
#define __PARSER__ 
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <ctype.h>
#include <inttypes.h>
#include "journal.h"
#include "extendibleHashing.h"


/// Message types
typedef enum { Done, DefineSchema, Transaction, ValidationQueries, Flush, Forget } Type_t;

/// Support operations
typedef enum { Equal, NotEqual, Less, LessOrEqual, Greater, GreaterOrEqual } Op_t;

typedef enum { False, True } Boolean_t;

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
typedef struct Column {
  /// The column id
  uint32_t column;
  /// The operations
  Op_t op;
  /// The constant
  uint64_t value;
} Column_t;

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


// void skipWhiteSpaces(char**);
// char* getFirstToken(char**);


Journal_t** processDefineSchema(DefineSchema_t *, int*);

void processTransaction(Transaction_t *, Journal_t**);

void processValidationQueries(ValidationQueries_t *, Journal_t**);

void processFlush(Flush_t *, Journal_t**);

void processForget(Forget_t *, Journal_t**);


Forget_t* forgetParser(char **);

#endif
