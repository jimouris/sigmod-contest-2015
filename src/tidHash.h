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
typedef uint64_t Key;
typedef struct JournalRecord_t JournalRecord_t;
typedef struct Journal_t Journal_t;

typedef struct RangeArrayElement {
	uint64_t transaction_id;
	uint64_t rec_offset; /* like a pointer, but not one to avoid realloc problem */
} RangeArray;

typedef struct SubBucket {
	Key key;
	uint64_t current_entries;
	uint64_t limit;
	RangeArray *transaction_range;
} SubBucket;

typedef struct Bucket { /* Has a pointer (key_buckets) to one or more subBuckets */
	uint32_t local_depth;
	uint32_t current_subBuckets;
	SubBucket *key_buckets;
	unsigned char deletion_started;
	uint64_t pointers_num;
} Bucket;

typedef struct pkHash {
	uint64_t size;
	uint32_t global_depth;
	Bucket **index;
} pkHash;
/****************************/

/* HASH INIT METHOD */
pkHash* createHash(); 
/********************/

/*HASH FUNCTION USED FOR TO GO TO THE RIGHT INDEX*/
uint64_t hashFunction(uint64_t, uint64_t);
/*************************************************/

/*INSERT TO HASH FUNCTION AND OTHER HELPER FUNCTIONS*/
void fixHashPointers(Bucket **, Bucket *, uint32_t, uint64_t);
void fixSplitPointers(pkHash *, Bucket *, Bucket *, uint64_t);
void duplicateIndex(pkHash *);
void addNewKeyToTempBucket(Bucket *,JournalRecord_t*);
void copyBucketTransactions(Bucket*, Bucket*);
void copySubbucketTransactions(SubBucket*, SubBucket*);
int insertHashRecord(pkHash*, Key, RangeArray*);
Bucket* createNewBucket(uint32_t);
void addNewKeyToTmpBucket(Bucket *, Key, RangeArray*);
void cleanBucket(Bucket *);
void cleanSubBucket(SubBucket *);
/****************************************************/

/*SEARCH TO HASH AND OTHER HELPER METHODS*/
RangeArray* getHashRecord(pkHash *, Key, uint64_t *);
JournalRecord_t* getLastRecord(Journal_t*, Key);
void moveSubBucketsLeft(Bucket*,uint32_t);
// List<Record> getHashRecords(pkHash*, Key, int range_start, int range_end);
/****************************************/

/*PRINT HASH-BUCKET FUNCTIONS*/
void printHash(pkHash*);
void printBucket(Bucket *);
/****************************/

/*DELETE HASH FUNCTION*/
int deleteHashRecord(pkHash*, Key);
int deleteSubBucket(pkHash*, uint64_t, Key);
void destroyBucket(Bucket*);
void tryMergeBuckets(pkHash*, uint64_t);
void fixDeletePointers(pkHash* , Bucket* , Bucket* , uint64_t);
unsigned char tryCollapseIndex(pkHash*);
// OK_SUCCESS deleteJournalRecord(pkHash*, Key, int transaction_id); 
int destroyHash(pkHash*); 
/**********************/

#endif
