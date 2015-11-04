#ifndef __ExtendibleHashing__
#define __ExtendibleHashing__ 

#include <stdio.h>
#include <stdlib.h>
#include <inttypes.h>
#include <errno.h>
#include <string.h>


#define GLOBAL_DEPTH_INIT 1
#define C 4
#define ALLOCATION_ERROR(X) if (X == NULL) { \
							perror("allocation error"); \
							exit(EXIT_FAILURE);} \

typedef uint64_t Key;

typedef struct Transaction_ptr {
	uint64_t transaction_id;

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
void splitBucket(Bucket **, uint64_t, uint64_t, size_t, size_t);
void fixHashPointers(Bucket **, Bucket *, size_t, uint64_t, size_t);

Hash* createHash(); 

int insertHashRecord(Hash*, Key, RangeArray*, uint64_t);

int deleteHashRecord(Hash*, Key);

// OK_SUCCESS deleteJournalRecord(Hash*, Key, int transaction_id); 

RangeArray* getHashRecord(Hash*, Key); 

// List<Record> getHashRecords(Hash*, Key, int range_start, int range_end);

// OK_SUCCESS destroyHash(Hash*); 

#endif
