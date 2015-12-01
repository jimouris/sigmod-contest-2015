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

void tidDuplicateIndex(tidHash *);

int tidDestroyHash(tidHash*); 
/**********************/

#endif
