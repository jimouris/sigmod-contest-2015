#ifndef __PK_HASH__
#define __PK_HASH__ 

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

typedef struct pkSubBucket {
	Key key;
	uint64_t current_entries;
	uint64_t limit;
	RangeArray *transaction_range;
} pkSubBucket;

typedef struct pkBucket { /* Has a pointer (key_buckets) to one or more subBuckets */
	uint32_t local_depth;
	uint32_t current_subBuckets;
	pkSubBucket **key_buckets;
	uint8_t deletion_started;
	uint64_t pointers_num;
} pkBucket;

typedef struct pkHash {
	uint64_t size;
	uint32_t global_depth;
	pkBucket **index;
} pkHash;

/********************************************************/
pkHash* createHash(void); 
uint64_t hashFunction(uint64_t, uint64_t);
void fixHashPointers(pkBucket **, pkBucket *, uint32_t, uint64_t);
void fixSplitPointers(pkHash *, pkBucket *, pkBucket *, uint64_t);
void duplicateIndex(pkHash *);
void addNewKeyToTempBucket(pkBucket *,JournalRecord_t*);
void copyBucketPtrs(pkBucket*, pkBucket*);
int insertHashRecord(pkHash*, Key, RangeArray*);
pkBucket* createNewBucket(uint32_t);
pkSubBucket* createNewSubBucket(Key, RangeArray *);
void addNewKeyToTmpBucket(pkBucket *, Key, RangeArray*);
void cleanBucket(pkBucket *);
void cleanSubBucket(pkSubBucket *);
RangeArray* getHashRecord(pkHash *, Key, uint64_t *);
JournalRecord_t* getLastRecord(Journal_t*, Key);
void printHash(pkHash*);
void printBucket(pkBucket *);
int destroyHash(pkHash*); 

#endif
