#ifndef __ExtendibleHashing__
#define __ExtendibleHashing__ 
#include <stdio.h>
#include <stdlib.h>
#include <inttypes.h>

#define GLOBAL_DEPTH_INIT 1
#define C 4
#define MALLOC_ERROR(X) if (X == NULL) { \
							perror("malloc"); \
							exit(EXIT_FAILURE);} \

typedef struct Transaction_ptr {
	uint64_t transaction_id;

} t_t;

typedef struct Bucket {
	size_t local_depth;
	size_t capacity;
	t_t *transaction_range;
} Bucket;

typedef struct Hash {
	size_t size;
	size_t global_depth;
	Bucket **index;
} Hash;

Hash* createHash(); 

// OK_SUCCESS insertHashRecord(Hash*, Key, RangeArray*);  

// OK_SUCCESS deleteHashRecord(Hash*, Key);

// OK_SUCCESS deleteJournalRecord(Hash*, Key, int transaction_id); 

// RangeArray* getHashRecord(Hash*, Key); 

// List<Record> getHashRecords(Hash*, Key, int range_start, int range_end);

// OK_SUCCESS destroyHash(Hash*); 

#endif
