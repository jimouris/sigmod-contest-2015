#ifndef __PARSER__
#define __PARSER__ 
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <ctype.h>
#include <inttypes.h>
#include "journal.h"
#include "PKeyHash.h"



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

typedef struct Val_list_node {
   ValidationQueries_t* data;
   struct Val_list_node *next;
} Val_list_node;

typedef struct Val_list_t {
   Val_list_node *list_beg;
   Val_list_node *list_end;
   uint64_t size;
} Val_list_t;


typedef struct ValidationList {
   Val_list_t* list;
	uint64_t num_of_validations;
	uint64_t capacity;
} ValidationList_t;


Boolean_t validation_isEmpty(Val_list_t*);

Column_t** removeDuplicates(Column_t** old, uint64_t old_size, uint64_t* new_size);

void destroy_validation_list(Val_list_t*);

List_node* validation_insert_start(Val_list_t* l_info, ValidationQueries_t* d);

void validation_insert_end(Val_list_t* l_info, ValidationQueries_t* d);

void validation_remove_end(Val_list_t* l_info);

void validation_remove_start(Val_list_t *list);

Val_list_t* validation_list_create();

void validation_print_list(Val_list_t *l_info);


int validationListInsert(ValidationList_t* , ValidationQueries_t*);

void validationListPrint(ValidationList_t*);

ValidationList_t* validationListCreate();

void validationListDestroy(ValidationList_t*);

Journal_t** processDefineSchema(DefineSchema_t *, int*, Boolean_t*);

void processTransaction(Transaction_t *, Journal_t**);

void processValidationQueries(ValidationQueries_t *, Journal_t**, ValidationList_t*);

void processFlush(Flush_t *, Journal_t**, ValidationList_t*);

Boolean_t checkValidation(Journal_t**, ValidationQueries_t*);

Boolean_t checkSingleQuery(Journal_t**, Query_t*, uint64_t, uint64_t);
Boolean_t checkQueryHash(Journal_t **, Query_t *, uint64_t, uint64_t);

void processForget(Forget_t *, Journal_t**);

void destroySchema(Journal_t**, int);

void printValidation(ValidationQueries_t*);

// int cmp_col(const void *, const void *);

// Boolean_t equal_col(const void *p1, const void *p2);


#endif
