#ifndef __PREDICATE_HASH__
#define __PREDICATE_HASH__ 

#include <stdio.h>
#include <stdlib.h>
#include <inttypes.h>
#include <errno.h>
#include <string.h>
#include "journal.h"
#include "constants.h"


typedef struct predicateSubBucket {
	uint64_t range_start;
	uint64_t range_end;
	Column_t condition;
	Boolean_t conflict;
} predicateSubBucket;

typedef struct predicateBucket {
	uint8_t deletion_started;
	uint32_t local_depth;
	uint32_t current_subBuckets;
	uint64_t pointers_num;
	predicateSubBucket *key_buckets;
} predicateBucket;

typedef struct predicateHash {
	uint64_t size;
	uint32_t global_depth;
	predicateBucket **index;
} predicateHash;


#endif