#ifndef __TID_HASH__
#define __TID_HASH__ 

#include <stdio.h>
#include <stdlib.h>
#include <inttypes.h>
#include <errno.h>
#include <string.h>
#include "constants.h"

typedef struct tidSubBucket {
	uint64_t transaction_id;
	uint64_t rec_offset;
} tidSubBucket;

typedef struct tidBucket { /* Has a pointer (key_buckets) to one or more subBuckets */
	uint8_t deletion_started;
	uint32_t local_depth;
	uint32_t current_subBuckets;
	uint64_t pointers_num;
	tidSubBucket *key_buckets;
} tidBucket;

typedef struct tidHash {
	uint64_t size;
	uint32_t global_depth;
	tidBucket **index;
} tidHash;
/****************************/

/* HASH INIT METHOD */
tidHash* tidCreateHash(); 
/********************/

/*HASH FUNCTION USED FOR TO GO TO THE RIGHT INDEX*/
uint64_t tidHashFunction(uint64_t, uint64_t);
/*************************************************/

/*INSERT TO HASH FUNCTION AND OTHER HELPER FUNCTIONS*/
void tidFixHashPointers(tidBucket **, tidBucket *, uint32_t, uint64_t); 
void tidFixSplitPointers(tidHash *, tidBucket *, tidBucket *, uint64_t);
void tidDuplicateIndex(tidHash *);
void tidCopyBucketTransactions(tidBucket* , tidBucket*);
void tidCopySubbucketTransactions(tidSubBucket*, tidSubBucket*);
int tidInsertHashRecord(tidHash* , tidSubBucket*);
tidBucket* tidCreateNewBucket(uint32_t);
void tidCleanBucket(tidBucket *);
void tidCleanSubBucket(tidSubBucket *);
/****************************************************/

/*SEARCH TO HASH AND OTHER HELPER METHODS*/
uint64_t tidGetHashOffset(tidHash *, uint64_t, Boolean_t *);
/****************************************/

/*PRINT HASH-tidBUCKET FUNCTIONS*/
void tidPrintBucket(tidBucket *);
void tidPrintHash(tidHash *);
/****************************/

/*DELETE HASH FUNCTION*/
int tidDestroyHash(tidHash *); 
void tidDestroyBucket(tidBucket *);
/**********************/


#endif
