#ifndef __TID_HASH__
#define __TID_HASH__ 

#include <stdio.h>
#include <stdlib.h>
#include <inttypes.h>
#include <errno.h>
#include <string.h>
#include "constants.h"
#include "journal.h"

/* HASH DATA STRUCTURES */
typedef struct JournalRecord_t JournalRecord_t;
typedef struct Journal_t Journal_t;

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
	tidBucket **tidIndex;
} tidHash;
/****************************/

/* HASH INIT METHOD */
tidHash* createHash(); 
/********************/

/*HASH FUNCTION USED FOR TO GO TO THE RIGHT INDEX*/
uint64_t hashFunction(uint64_t, uint64_t);
/*************************************************/

/*INSERT TO HASH FUNCTION AND OTHER HELPER FUNCTIONS*/
void fixHashPointers(tidBucket **, tidBucket *, uint32_t, uint64_t);
void fixSplitPointers(tidHash *, tidBucket *, tidBucket *, uint64_t);
void duplicateIndex(tidHash *);
void addNewKeyToTempBucket(tidBucket *,JournalRecord_t*);
void copyBucketTransactions(tidBucket*, tidBucket*);
void copySubbucketTransactions(tidSubBucket*, tidSubBucket*);
int insertHashRecord(tidHash*, Key, RangeArray*);
tidBucket* createNewBucket(uint32_t);
void addNewKeyToTmpBucket(tidBucket *, Key, RangeArray*);
void cleanBucket(tidBucket *);
void cleanSubBucket(tidSubBucket *);
/****************************************************/

/*SEARCH TO HASH AND OTHER HELPER METHODS*/
RangeArray* getHashRecord(tidHash *, Key, uint64_t *);
JournalRecord_t* getLastRecord(Journal_t*, Key);
void moveSubBucketsLeft(tidBucket*,uint32_t);
// List<Record> getHashRecords(tidHash*, Key, int range_start, int range_end);
/****************************************/

/*PRINT HASH-tidBUCKET FUNCTIONS*/
void printHash(tidHash*);
void printBucket(tidBucket *);
/****************************/

/*DELETE HASH FUNCTION*/
int deleteHashRecord(tidHash*, Key);
int deleteSubBucket(tidHash*, uint64_t, Key);
void destroyBucket(tidBucket*);
void tryMergeBuckets(tidHash*, uint64_t);
void fixDeletePointers(tidHash* , tidBucket* , tidBucket* , uint64_t);
uint8_t tryCollapseIndex(tidHash*);
// OK_SUCCESS deleteJournalRecord(tidHash*, Key, int transaction_id); 
int destroyHash(tidHash*); 
/**********************/

#endif
