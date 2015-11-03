#include "extendibleHashing.h"


Hash* createHash() {
	Hash *hash = malloc(sizeof(Hash));
	ALLOCATION_ERROR(hash);
	hash->global_depth = GLOBAL_DEPTH_INIT;
	hash->size = 1 << GLOBAL_DEPTH_INIT;
	hash->index = malloc(hash->size * sizeof(Bucket *));
	ALLOCATION_ERROR(hash->index);
	int i, j;
	for (i = 0 ; i < hash->size ; i++) {
		hash->index[i] = malloc(sizeof(Bucket));
		ALLOCATION_ERROR(hash->index[i]);
		hash->index[i]->local_depth = GLOBAL_DEPTH_INIT;
		hash->index[i]->capacity = C;
		hash->index[i]->current_entries = 0 ;
		size_t capacity = hash->index[i]->capacity;
		hash->index[i]->transaction_range = malloc(capacity * sizeof(t_t));
		ALLOCATION_ERROR(hash->index[i]->transaction_range);
		// o pinakas transaction range exei skata mesa
	}
	return hash;
}

void splitBucket(Bucket **index, uint64_t bucket_num) {
	Bucket *new_bucket = malloc(sizeof(Bucket));
	ALLOCATION_ERROR(new_bucket);
	new_bucket->capacity = C;
	index[bucket_num]->local_depth++;
	new_bucket->local_depth = index[bucket_num]->local_depth;
	new_bucket->current_entries = 0;
	new_bucket->transaction_range = malloc(new_bucket->capacity * sizeof(t_t));
	ALLOCATION_ERROR(new_bucket->transaction_range);

	Bucket *tmp_bucket = malloc(sizeof(Bucket));
	ALLOCATION_ERROR(tmp_bucket);
	tmp_bucket->capacity = C;
	tmp_bucket->transaction_range = malloc((tmp_bucket->capacity+1) * sizeof(t_t));
	ALLOCATION_ERROR(tmp_bucket->transaction_range);

	memcpy(tmp_bucket, index[bucket_num], sizeof(Bucket));


}

int insertHashRecord(Hash* hash, Key key, RangeArray* rangeArray) {
	uint64_t bucket_num = hashFunction(hash->size, key);
	Bucket *bucket = hash->index[bucket_num];
	if (bucket->current_entries < bucket->capacity) { // If there is space to insert it on the bucket
		size_t current_entries = bucket->current_entries; 
		bucket->transaction_range[current_entries].transaction_id = key;
		bucket->current_entries++;
		return 0; // OK_SUCCESS
	} else { // if there is no space
		if (bucket->local_depth == hash->global_depth) { // one pointer per bucket -> doublicate index
			hash->global_depth++;
			hash->size *= 2;
			hash->index = realloc(hash->index, hash->size * sizeof(Bucket *));
			ALLOCATION_ERROR(hash->index);
			//split
			//fix pointers
		} else if (bucket->local_depth < hash->global_depth) { // split bucket

		}
	}

}  

uint64_t hashFunction(uint64_t size, uint64_t n) {
	return n % size;
    // unsigned long value = 0 ; 
    // int shortvalue = 0 ;
    // int lastfour = 0 ;
    // int middletwo = 0;
    // int firsttwo = 0 ;
    // int i = 0;
    // for (i = 2 ; i<strlen(feed); i++) {
    //     if (i==2 || i==3)
    //     {
    //         firsttwo = firsttwo * 10 + (feed[i] - '0');
    //     }
    //     else if(i == 4 || i ==5)
    //     {
    //         middletwo = middletwo * 10 + (feed[i] - '0');
    //     }
    //     else if (i==6||i==7||i==8 || i==9)
    //     {
    //         lastfour = lastfour * 10 + (feed[i] - '0'); //get the value of last 4 digits
    //     }
    // }
    // value = 9293 * (1201*firsttwo + middletwo + lastfour);
    // return (value % n);
}

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
