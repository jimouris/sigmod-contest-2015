#ifndef __PARSER__
#define __PARSER__ 
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <inttypes.h>

typedef enum { Equal, NotEqual, Less, LessOrEqual, Greater, GreaterOrEqual } Op_t;
typedef enum { Done, DefineSchema, Transaction, ValidationQueries, Flush, Forget } Type_t;

typedef struct MessageHead { 
	uint32_t messageLen;	/// Total message length, excluding this head 
	Type_t type;			/// The message type 
} MessageHead_t; 

typedef struct DefineSchema { 
	/// Number of relations 
	uint32_t relationCount; 
	/// Column counts per relation, one count per relation.  
	/// The first column is always the primary key 
	uint32_t columnCounts[]; 
} DefineSchema_t;

typedef struct Transaction { 
	uint64_t transactionId;	/// The transaction id. Monotonic increasing 
	uint32_t insertCount;	/// The operation counts 
	uint32_t deleteCount;	/// The operation counts 
	char operations[];		/// A sequence of transaction operations. Deletes first 
} Transaction_t; 

typedef struct TransactionOperationDelete { 
	uint32_t relationId;	/// The affected relation 
	uint32_t rowCount;		/// The row count 
	uint64_t keys[];		/// The deleted values, rowCount primary keyss 
} TransactionOperationDelete_t; 

typedef struct TransactionOperationInsert { 
	uint32_t relationId;	/// The affected relation 
	uint32_t rowCount;		/// The row count 
	uint64_t values[];		/// The inserted values, rowCount*relation[relationId].columnCount values 
} TransactionOperationInsert_t;

typedef struct Forget { 
	/// Transactions older than that (including) will not be tested for 
	uint64_t transactionId; 
} Forget_t; 

typedef struct ValidationQueries { 
	uint64_t validationId;	/// The validation id. Monotonic increasing 
	uint64_t from,to;		/// The transaction range 
	uint32_t queryCount;	/// The query count 
	char queries[];			/// The queries 
} ValidationQueries_t; 

typedef struct Column { 
	uint32_t column;	/// The column id 
	Op_t op;			/// The operations 
	uint64_t value;		/// The constant 
} Column_t; 

typedef struct Query { 
	uint32_t relationId;	/// The relation 
	uint32_t columnCount;	/// The number of bound columns 
	Column_t columns[];		/// The bindings 
} Query_t;

typedef struct Flush { 
	/// All validations to this id (including) must be answered 
	uint64_t validationId; 
} Flush_t;


DefineSchema_t* defineScemaParser(char **);


#endif
