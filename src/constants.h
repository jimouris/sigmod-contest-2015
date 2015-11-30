#ifndef __CONSTANTS__
#define __CONSTANTS__ 

#define GLOBAL_DEPTH_INIT 1			/* Hash init depth */
#define B 1							/* Number of keys for each bucket */
#define C 4							/* Initial capacity for transactions in bucket */
#define JOURNAL_CAPACITY_INIT 64	
#define VALIDATION_COUNT_INIT 64	
#define OK_SUCCESS 0
#define NOK_FAILURE 1

#define ALLOCATION_ERROR(X) if (X == NULL) { \
							perror("allocation error"); \
							exit(EXIT_FAILURE);} \



typedef enum { False = 0, True = 1 } Boolean_t;


/// Message types
typedef enum { Done, DefineSchema, Transaction, ValidationQueries, Flush, Forget } Type_t;

/// Support operations
typedef enum { Equal, NotEqual, Less, LessOrEqual, Greater, GreaterOrEqual } Op_t;


#endif
