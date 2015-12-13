#ifndef __PREDICATE_HASH__
#define __PREDICATE_HASH__ 

#include <stdio.h>
#include <stdlib.h>
#include <inttypes.h>
#include <errno.h>
#include <string.h>
#include "journal.h"
#include "bitSet.h"
#include "constants.h"

typedef struct Column Column_t;

typedef struct predicateSubBucket {
	uint64_t range_start;
	uint64_t range_end;
	Column_t *condition;
	uint8_t* bit_set;
	uint64_t bit_set_size;
	uint64_t open_requests;
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



/* HASH INIT METHOD */
predicateHash* predicateCreateHash(); 
/********************/

/*HASH FUNCTION USED FOR TO GO TO THE RIGHT INDEX*/
uint64_t predicateHashFunction(uint64_t, predicateSubBucket*);
/*************************************************/

/*INSERT TO HASH FUNCTION AND OTHER HELPER FUNCTIONS*/
void predicateFixHashPointers(predicateBucket **, predicateBucket *, uint32_t, uint64_t); 
void predicateFixSplitPointers(predicateHash*, predicateBucket*, predicateBucket*, uint64_t );
void predicateDuplicateIndex(predicateHash*);
void predicateCopyBucketTransactions(predicateBucket*, predicateBucket*);
void predicateCopySubbucketTransactions(predicateSubBucket*, predicateSubBucket*);
int predicateInsertHashRecord(predicateHash*, predicateSubBucket*);
predicateBucket* predicateCreateNewBucket(uint32_t);
void preidcateCleanBucket(predicateBucket *);
void predicateCleanSubBucket(predicateSubBucket *);
Boolean_t predicateRecordsEqual(predicateSubBucket*, predicateSubBucket*);
/*****************************************************/

/*DELETE HASH FUNCTION*/
int predicateDestroyHash(predicateHash *); 
void predicateDestroyBucket(predicateBucket *);
void predicateCleanBucket(predicateBucket*);
/**********************/



#endif