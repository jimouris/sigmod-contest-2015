#ifndef __ExtendibleHashing__
#define __ExtendibleHashing__ 

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
} Bucket;

typedef struct Hash {
	uint64_t size;
	uint32_t global_depth;
	Bucket **index;
} Hash;
/****************************/

/* HASH INIT METHOD */
Hash* createHash(); 
/********************/

/*HASH FUNCTION USED FOR TO GO TO THE RIGHT INDEX*/
uint64_t hashFunction(uint64_t, uint64_t);
/*************************************************/

/*INSERT TO HASH FUNCTION AND OTHER HELPER FUNCTIONS*/
void fixHashPointers(Bucket **, Bucket *, uint32_t, uint64_t);
void doublicateIndex(Hash *);
void addNewKeyToTempBucket(Bucket *,JournalRecord_t*);
void copyBucketTransactions(Bucket*, Bucket*);
int insertHashRecord(Hash*, Key, RangeArray*);
Bucket* createNewBucket(uint32_t, uint32_t);
void addNewKeyToTmpBucket(Bucket *, Key, RangeArray*);
void cleanBucket(Bucket *);
/****************************************************/

/*SEARCH TO HASH AND OTHER HELPER METHODS*/
RangeArray* getHashRecord(Hash *, Key, uint64_t *);
JournalRecord_t* getLastRecord(Journal_t*, Key);
// List<Record> getHashRecords(Hash*, Key, int range_start, int range_end);
/****************************************/

/*PRINT HASH-BUCKET FUNCTIONS*/
void printHash(Hash*);
void printBucket(Bucket *);
/****************************/

/*DELETE HASH FUNCTION*/
int deleteHashRecord(Hash*, Key);
void destroyBucket(Bucket *bucket,uint32_t b);
// OK_SUCCESS deleteJournalRecord(Hash*, Key, int transaction_id); 
int destroyHash(Hash*); 
/**********************/

#endif
