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

void splitBucket(Bucket **index, uint64_t bucket_num, uint64_t tid, size_t global_depth, size_t split_only) {
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
	tmp_bucket->transaction_range[C].transaction_id = tid;
	// tmp_bucket[C].transaction_id = &tidInJournal;
	memset(index[bucket_num]->transaction_range, 0, sizeof(C * sizeof(t_t)));

	/*flags to check if the split actually changed tids or we have to doublicate index again*/
	int i;
	size_t flag1 = 0;
	size_t flag2 = 0;
	uint64_t new_bucket_hash = 0;
	for (i = 0 ; i <= C ; i++) { //scan the tmp_bucket
		uint64_t tmp_tid = tmp_bucket->transaction_range[i].transaction_id;
		uint64_t new_hash = hashFunction(1 << global_depth, tmp_tid);
		if (new_hash == bucket_num && index[bucket_num]->current_entries < C) { //tid on old bucket
			size_t current_entries = index[bucket_num]->current_entries;
			index[bucket_num]->transaction_range[current_entries].transaction_id = tmp_tid;
			index[bucket_num]->current_entries++;
			flag1 = 1;
		} else if (new_hash != bucket_num && new_bucket->current_entries < C) { //tid on new bucket
			size_t current_entries = new_bucket->current_entries;
			new_bucket->transaction_range[current_entries].transaction_id = tmp_tid;
			new_bucket->current_entries++;
			flag2 = 1;
			new_bucket_hash = new_hash;
		}
	}
	fixHashPointers(index, new_bucket, global_depth, bucket_num, split_only);
	free(tmp_bucket->transaction_range);
	free(tmp_bucket);
	// if all entries gone to one bucket 
	/* MAS LEIPEI KATI*/
	if (flag1 == 0) 
		splitBucket(index, new_bucket_hash, tid, global_depth, 0);		
	else if (flag2 == 0)
		splitBucket(index, bucket_num, tid, global_depth, 0);		
}

/*fix new indexes pointers after doublicates*/
void fixHashPointers(Bucket **index, Bucket *new_bucket, size_t global_depth, uint64_t bucket_num, size_t split_only) {
	int i, j;
	size_t old_size = 1 << (global_depth-1);
	if (split_only) {
		index[bucket_num+old_size] = new_bucket;
	} else {
		for (i = 0, j = old_size ; i < old_size ; i++, j++) {
			if (i == bucket_num)
				index[j] = new_bucket;
			else
				index[j] = index[i];
		}
	}
}

int insertHashRecord(Hash* hash, Key key, RangeArray* rangeArray, uint64_t tid) {
	uint64_t bucket_num = hashFunction(hash->size, tid);
	Bucket *bucket = hash->index[bucket_num];
	if (bucket->current_entries < bucket->capacity) { // If there is space to insert it on the bucket
		size_t current_entries = bucket->current_entries; 
		bucket->transaction_range[current_entries].transaction_id = tid; // einai arxidia, anti gia key, 8elei Tid
		bucket->current_entries++;
		return 0; // OK_SUCCESS
	} else { // if there is no space
		if (bucket->local_depth == hash->global_depth) { // one pointer per bucket -> doublicate index
			hash->global_depth++;
			hash->size *= 2;
			hash->index = realloc(hash->index, hash->size * sizeof(Bucket *));
			ALLOCATION_ERROR(hash->index);
			splitBucket(hash->index, bucket_num, tid, hash->global_depth, 0);
		} else if (bucket->local_depth < hash->global_depth) { // split bucket
			splitBucket(hash->index, bucket_num, tid, hash->global_depth, 1);
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
