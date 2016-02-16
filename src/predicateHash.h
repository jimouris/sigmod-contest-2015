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
typedef struct Zombie_node Zombie_node;
typedef struct ZombieList ZombieList;

typedef struct predicateSubBucket {
	uint64_t range_start;
	uint64_t range_end;
	Column_t *condition;
	BitSet_t* bit_set;
	uint64_t open_requests;
	uint64_t last_validationId;
	Zombie_node *zombie;
} predicateSubBucket;

typedef struct predicateBucket {
	uint8_t deletion_started;
	uint32_t local_depth;
	uint32_t current_subBuckets;
	uint64_t pointers_num;
	predicateSubBucket **key_buckets;
} predicateBucket;

typedef struct predicateHash {
	uint64_t size;
	uint32_t global_depth;
	predicateBucket **index;
	ZombieList* zombie_list;
} predicateHash;

typedef struct PredicateRangeArray {
	uint64_t from;
	uint64_t to; 
	uint32_t column;
	Op_t op;
	uint64_t value;
} PredicateRangeArray;

typedef struct Zombie_node {
   predicateSubBucket* subBucket;
   struct Zombie_node *next;
   struct Zombie_node *prev;
} Zombie_node;

typedef struct ZombieList {
  	Zombie_node* first_zombie;
  	Zombie_node* last_zombie;
	uint64_t num_of_zombies;
} ZombieList;

/*Zombie functions*/
ZombieList* zombieList_create(void);
Zombie_node * zombieList_insert_end(ZombieList*, predicateSubBucket*);
void zombieList_remove(ZombieList *, Zombie_node *);
void zombieList_destroy(ZombieList *);
/*HASH*/
predicateHash* predicateCreateHash(void); 
uint64_t predicateHashFunction(uint64_t, uint64_t, uint64_t, uint32_t, Op_t op, uint64_t);
void predicateFixHashPointers(predicateBucket **, predicateBucket *, uint32_t, uint64_t); 
void predicateFixSplitPointers(predicateHash*, predicateBucket*, predicateBucket*, uint64_t );
void predicateDuplicateIndex(predicateHash*);
void predicateCopyBucketPtrs(predicateBucket*, predicateBucket*);
void predicateCopySubbucketTransactions(predicateSubBucket*, predicateSubBucket*);
int predicateInsertHashRecord(predicateHash*, uint64_t, uint64_t, uint32_t, Op_t, uint64_t, uint64_t);
predicateBucket* predicateCreateNewBucket(uint32_t);
predicateSubBucket* predicateCreateNewSubBucket(predicateSubBucket *);
void predicateCleanBucket(predicateBucket *);
bool predicateRecordsEqual(predicateSubBucket*, predicateSubBucket*);
BitSet_t* predicateGetBitSet(predicateHash *, uint64_t, uint64_t, uint32_t, Op_t op, uint64_t);
void predicatePrintHash(predicateHash *);
void predicatePrintBucket(predicateBucket *);
predicateSubBucket* createPredicateSubBucket(uint64_t, uint64_t, uint32_t, Op_t, uint64_t, uint64_t);
bool predicateRecordsEqualArguements(predicateSubBucket*,  uint64_t, uint64_t, uint32_t, Op_t, uint64_t);
int predicateInsertBitSet(predicateHash*, uint64_t, uint64_t, uint32_t, Op_t, uint64_t, BitSet_t*);
/*FORGET RELATED FUNCTIONS*/
int predicateDeleteHashRecord(predicateHash*, predicateSubBucket*);
int predicateForgetSubBucket(predicateHash*, uint64_t, predicateSubBucket*);
void predicateMoveSubBucketsLeft(predicateBucket*, uint32_t);
void predicateTryMergeBuckets(predicateHash*, uint64_t);
uint8_t predicateTryCollapseIndex(predicateHash*);
void forgetPredicateIndex(predicateHash*, uint64_t);
/*DELETE HASH FUNCTION*/
void predicateDestroyBucket(predicateBucket *);
void predicateDestroySubBucket(predicateSubBucket *);
int predicateDestroyHash(predicateHash *);
void predicateFixDeletePointers(predicateHash*, predicateBucket*, predicateBucket* , uint64_t);
void predicateDestroyBucketNoSubBuckets(predicateBucket *bucket);

#endif