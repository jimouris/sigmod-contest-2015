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
	uint32_t local_depth;
	uint32_t current_subBuckets;
	tidSubBucket *key_buckets;
	uint8_t deletion_started;
	uint64_t pointers_num;
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
uint64_t tidHashFunction(uint64_t, uint64_t);
tidBucket* tidCreateNewBucket(uint32_t);
int tidInsertHashRecord(tidHash* , tidSubBucket*);
void tidCopyBucketTransactions(tidBucket* , tidBucket*);
void tidCopySubbucketTransactions(tidSubBucket*, tidSubBucket*);
void tidDuplicateIndex(tidHash *);
void tidCleanBucket(tidBucket *);
void tidCleanSubBucket(tidSubBucket *);
uint64_t tidGetHashOffset(tidHash *, uint64_t, Boolean_t *);
uint64_t tidHashFunction(uint64_t, uint64_t);
void tidFixHashPointers(tidBucket **, tidBucket *, uint32_t, uint64_t); 
void tidFixSplitPointers(tidHash *, tidBucket *, tidBucket *, uint64_t);
int tidDestroyHash(tidHash *); 
void tidDestroyBucket(tidBucket *);
void tidPrintBucket(tidBucket *);
void tidPrintHash(tidHash *);
/**********************/

#endif
