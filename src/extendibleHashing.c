#include "extendibleHashing.h"
#include <errno.h>

Hash* createHash() {
	Hash *hash = malloc(sizeof(Hash));
	MALLOC_ERROR(hash);
	hash->global_depth = GLOBAL_DEPTH_INIT;
	hash->size = 1 << GLOBAL_DEPTH_INIT;
	hash->index = malloc(hash->size * sizeof(Bucket *));
	MALLOC_ERROR(hash->index);
	int i, j;
	for (i = 0 ; i < hash->size ; i++) {
		hash->index[i] = malloc(sizeof(Bucket));
		MALLOC_ERROR(hash->index[i]);
		hash->index[i]->local_depth = GLOBAL_DEPTH_INIT;
		hash->index[i]->capacity = C;
		size_t capacity = hash->index[i]->capacity;
		hash->index[i]->transaction_range = malloc(capacity * sizeof(t_t));
		MALLOC_ERROR(hash->index[i]->transaction_range);

	}
}

// OK_SUCCESS insertHashRecord(Hash*, Key, RangeArray*) {

// }  

// OK_SUCCESS deleteHashRecord(Hash*, Key) {

// } 

// OK_SUCCESS deleteJournalRecord(Hash*, Key, int transaction_id) {

// } 

// RangeArray* getHashRecord(Hash*, Key) {

// } 

// List<Record> getHashRecords(Hash*, Key, int range_start, int range_end) {

// }

// OK_SUCCESS destroyHash(Hash*) {

// } 
