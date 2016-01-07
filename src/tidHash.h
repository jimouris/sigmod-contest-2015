#ifndef __TID_HASH__
#define __TID_HASH__ 

#include <stdio.h>
#include <stdlib.h>
#include <inttypes.h>
#include <errno.h>
#include <stdbool.h>
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
	tidSubBucket **key_buckets;
} tidBucket;

typedef struct tidHash {
	uint64_t size;
	uint32_t global_depth;
	tidBucket **index;
} tidHash;
/****************************/

/* HASH INIT METHOD */
tidHash* tidCreateHash(void); 
/********************/

/*HASH FUNCTION USED FOR TO GO TO THE RIGHT INDEX*/
uint64_t tidHashFunction(uint64_t, uint64_t);
/*************************************************/

/*INSERT TO HASH FUNCTION AND OTHER HELPER FUNCTIONS*/
void tidFixHashPointers(tidBucket **, tidBucket *, uint32_t, uint64_t); 
void tidFixSplitPointers(tidHash *, tidBucket *, tidBucket *, uint64_t);
void tidDuplicateIndex(tidHash *);
void tidCopyBucketPtrs(tidBucket *, tidBucket *);
int tidInsertHashRecord(tidHash* , tidSubBucket*);
tidBucket* tidCreateNewBucket(uint32_t);
tidSubBucket* tidCreateNewSubBucket(tidSubBucket *);
void tidCleanBucket(tidBucket *);
/****************************************************/

/*SEARCH TO HASH AND OTHER HELPER METHODS*/
uint64_t tidGetHashOffset(tidHash *, uint64_t, bool *);
/****************************************/

/*DELETE HASH FUNCTION*/
int tidDestroyHash(tidHash *); 
void tidDestroyBucket(tidBucket *);
/**********************/


#endif
