#ifndef __ExtendibleHashing__
#define __ExtendibleHashing__ 

#include <stdio.h>
#include <stdlib.h>
#include <inttypes.h>
#include <errno.h>
#include <string.h>
#include "journal.h"

#define GLOBAL_DEPTH_INIT 1
#define C 4
#define ALLOCATION_ERROR(X) if (X == NULL) { \
							perror("allocation error"); \
							exit(EXIT_FAILURE);} \

typedef uint64_t Key;
typedef struct JournalRecord_t JournalRecord_t;

typedef struct Transaction_ptr {
	uint64_t transaction_id;
	JournalRecord_t * rec;
} t_t;

typedef t_t RangeArray;

typedef struct Bucket {
	size_t local_depth;
	size_t capacity;
	size_t current_entries; //use it for deciding split or insertion
	t_t *transaction_range;
} Bucket;

typedef struct Hash {
	uint64_t size;
	size_t global_depth;
	Bucket **index;
} Hash;

uint64_t hashFunction(uint64_t, uint64_t);
Hash* createHash(); 

/*INSERT TO HASH FUNCTION AND OTHER HELPER FUNCTIONS*/
void splitBucket(Hash*, uint64_t, JournalRecord_t*, size_t);
void fixHashPointers(Bucket **, Bucket *, size_t, uint64_t);
void doublicateIndex(Hash *);
int insertHashRecord(Hash*, Key, RangeArray*, JournalRecord_t*);
void addNewKeyToTempBucket(Bucket *,JournalRecord_t*);
/****************************************************/

/*PRINT FUNCTIONS*/
void printHash(Hash*);
void printBucket(Bucket *);
/****************/
int deleteHashRecord(Hash*, Key);

// OK_SUCCESS deleteJournalRecord(Hash*, Key, int transaction_id); 

RangeArray* getHashRecord(Hash*, Key); 

// List<Record> getHashRecords(Hash*, Key, int range_start, int range_end);

// OK_SUCCESS destroyHash(Hash*); 

#endif
