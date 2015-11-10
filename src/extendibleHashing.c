#include "extendibleHashing.h"

Hash* createHash() {
	Hash *hash = malloc(sizeof(Hash));
	ALLOCATION_ERROR(hash);
	hash->global_depth = GLOBAL_DEPTH_INIT;
	hash->size = 1 << GLOBAL_DEPTH_INIT;
	hash->index = malloc(hash->size * sizeof(Bucket *));
	ALLOCATION_ERROR(hash->index);
	uint64_t i;
	for (i = 0 ; i < hash->size ; i++)
		hash->index[i] = createNewBucket(GLOBAL_DEPTH_INIT, B);
	return hash;
}

/*insert Record to Hash*/
int insertHashRecord(Hash* hash, Key key, RangeArray* rangeArray) {
	uint64_t bucket_num = hashFunction(hash->size, key);
	Bucket *bucket = hash->index[bucket_num];
	uint32_t i;
	for (i = 0 ; i < bucket->current_subBuckets; i++) {			/* for all subbuckets */
		if (bucket->key_buckets[i].key == key) {				/* if there is a subBucket with this key */
			uint64_t current_entries = bucket->key_buckets[i].current_entries;
			if (current_entries >= C) { 	/* if there isn't enough free space, realloc */
				bucket->key_buckets[i].transaction_range = realloc(bucket->key_buckets[i].transaction_range, ((current_entries)+C) * sizeof(RangeArray));
				ALLOCATION_ERROR(bucket->key_buckets[i].transaction_range);
			}
			/* Insert entry */
			bucket->key_buckets[i].transaction_range[current_entries].transaction_id = rangeArray->transaction_id;
			bucket->key_buckets[i].transaction_range[current_entries].rec_offset = rangeArray->rec_offset;
			(bucket->key_buckets[i].current_entries)++;
			return OK_SUCCESS;
		}
	}
	/* If that was the first appearence of this key */
	if (bucket->current_subBuckets < B) {	/* If there is space to insert it on that bucket (there is a free subbucket) */
		bucket->key_buckets[bucket->current_subBuckets].key = key;
		bucket->key_buckets[bucket->current_subBuckets].transaction_range[0].transaction_id = rangeArray->transaction_id;
		bucket->key_buckets[bucket->current_subBuckets].transaction_range[0].rec_offset = rangeArray->rec_offset;
		bucket->key_buckets[bucket->current_subBuckets].current_entries += 1;
		(bucket->current_subBuckets)++;
		return OK_SUCCESS;
	} else {	/* if there is no space for a new key in this bucket */
		/* Doublicate index */
		if (bucket->local_depth == hash->global_depth) { // one pointer per bucket -> doublicate index
			splitBucket(hash, bucket_num, rangeArray, 1, key);
		} else if (bucket->local_depth < hash->global_depth) { // split bucket
			splitBucket(hash, bucket_num, rangeArray, 0, key);
		}

	}
	return OK_SUCCESS;
}  

/*Does whatever it says*/
void doublicateIndex(Hash * hash) {
	hash->global_depth++;
	uint64_t old_size = hash->size;
	hash->size *= 2;
	hash->index = realloc(hash->index, hash->size * sizeof(Bucket *));
	ALLOCATION_ERROR(hash->index);
	uint64_t i;
	for (i = old_size; i < hash->size; i++)
		hash->index[i] = NULL;
}

/* The main function of extendible hashing.
 * doublicate_index_flag -> insertHashRecord decide its value at first
 * bucket_num -> the overflow bucket
 * doublicate_index_flag -> is Set to 1 either on Recursion or from insertHashRecord depending
 * the local_depth of Bucket with the global depth
*/
void splitBucket(Hash* hash, uint64_t bucket_num, RangeArray* rangeArray, int doublicate_index_flag, Key key) {
	/* allocate a new tmp bucket and the new one (for the split) */
	if (doublicate_index_flag) {
		doublicateIndex(hash);	/* doublicates and increases global depth */
	}
	hash->index[bucket_num]->local_depth++;
	Bucket *new_bucket = createNewBucket(hash->index[bucket_num]->local_depth, B);
	Bucket *tmp_bucket = createNewBucket(hash->index[bucket_num]->local_depth, B+1);
	copyBucketTransactions(tmp_bucket, hash->index[bucket_num]);
	addNewKeyToTmpBucket(tmp_bucket, key, rangeArray);
	cleanBucket(hash->index[bucket_num]);
	/* flags to check if the split actually changed tids or we have to doublicate index again */
	uint32_t i;
	int flag1 = 0, flag2 = 0;
	uint64_t j, new_bucket_hash = 0;
	for (i = 0 ; i <= B ; i++) {	/* for all subBuckets in tmpBucket */
		uint64_t new_hash = hashFunction(1 << hash->global_depth, tmp_bucket->key_buckets[i].key); /* new_hash has a bucket number */
		if (new_hash == bucket_num && hash->index[bucket_num]->current_subBuckets < B) {	/* if the new bucket is the previous one and there is available subBucket space */
			uint32_t current_subBuckets = hash->index[new_hash]->current_subBuckets;
			hash->index[new_hash]->key_buckets[current_subBuckets].key = tmp_bucket->key_buckets[i].key;
			hash->index[new_hash]->key_buckets[current_subBuckets].current_entries = tmp_bucket->key_buckets[i].current_entries;
			uint64_t current_entries = hash->index[new_hash]->key_buckets[current_subBuckets].current_entries;
			for (j = 0 ; j < current_entries ; j++) {	/* for j in transactionRange of the bucket to be copied */
				hash->index[new_hash]->key_buckets[current_subBuckets].transaction_range[j].transaction_id = tmp_bucket->key_buckets[i].transaction_range[j].transaction_id;
				hash->index[new_hash]->key_buckets[current_subBuckets].transaction_range[j].rec_offset = tmp_bucket->key_buckets[i].transaction_range[j].rec_offset;
			}
			hash->index[new_hash]->current_subBuckets++;
			flag1 = 1;
		} else if (new_hash != bucket_num && new_bucket->current_subBuckets < B) {
			uint32_t current_subBuckets = new_bucket->current_subBuckets;
			new_bucket->key_buckets[current_subBuckets].key = tmp_bucket->key_buckets[i].key;
			new_bucket->key_buckets[current_subBuckets].current_entries = tmp_bucket->key_buckets[i].current_entries;
			uint64_t current_entries = new_bucket->key_buckets[current_subBuckets].current_entries;
			for(j = 0 ; j < current_entries ; j++ ) {
				new_bucket->key_buckets[current_subBuckets].transaction_range[j].transaction_id = tmp_bucket->key_buckets[i].transaction_range[j].transaction_id;
				new_bucket->key_buckets[current_subBuckets].transaction_range[j].rec_offset = tmp_bucket->key_buckets[i].transaction_range[j].rec_offset;
			}
			new_bucket->current_subBuckets++;
			new_bucket_hash = new_hash;
			flag2 = 1;
			hash->index[new_hash] = new_bucket;
		}
	}
	if (doublicate_index_flag) {
		fixHashPointers(hash->index, new_bucket, hash->global_depth, bucket_num);
	}
	/* free tmpBucket */
	for (i = 0 ; i <= B ; i++) { /*for each subBucket*/
		free(tmp_bucket->key_buckets[i].transaction_range);
	}
	free(tmp_bucket->key_buckets);
	free(tmp_bucket);
	tmp_bucket = NULL;
	// if all entries gone to one bucket 
	if (flag1 == 0) {
		splitBucket(hash, new_bucket_hash, rangeArray, 1, key);
	} else if (flag2 == 0) {
		splitBucket(hash, bucket_num, rangeArray, 1, key);
	}
}

/* fix new indexes pointers after index doublicate or just splits pointers */
void fixHashPointers(Bucket **index, Bucket *new_bucket, uint32_t global_depth, uint64_t bucket_num) {
	int i, j;
	uint64_t old_size = 1 << (global_depth-1);
	for (i = 0, j = old_size ; i < old_size ; i++, j++) {
		if (i == bucket_num)
			index[j] = new_bucket;
		else
			index[j] = index[i];
	}
}

/* copy transactions from one Bucket to another one*/
void copyBucketTransactions(Bucket* dst, Bucket* src) {
	uint64_t i;
	uint32_t j;
	dst->local_depth = src->local_depth;
	dst->current_subBuckets = src->current_subBuckets;
	for (i = 0 ; i < src->current_subBuckets ; i++) {	/* for i in subBuckets */
		dst->key_buckets[i].key = src->key_buckets[i].key;
		dst->key_buckets[i].current_entries = src->key_buckets[i].current_entries;
		for (j = 0 ; j < src->key_buckets[i].current_entries ; j++) { /* for j in transactionRange */
			dst->key_buckets[i].transaction_range[j].transaction_id = src->key_buckets[i].transaction_range[j].transaction_id;
			dst->key_buckets[i].transaction_range[j].rec_offset = src->key_buckets[i].transaction_range[j].rec_offset;
		}
	}
}

/* creates an empty Bucket*/
Bucket* createNewBucket(uint32_t local_depth, uint32_t b_size) {
	Bucket *new_bucket = malloc(sizeof(Bucket));
	ALLOCATION_ERROR(new_bucket);
	new_bucket->local_depth = local_depth;
	new_bucket->current_subBuckets = 0;
	new_bucket->key_buckets = malloc(b_size * sizeof(SubBucket));
	ALLOCATION_ERROR(new_bucket->key_buckets);
	uint32_t i, j;
	for (i = 0 ; i < b_size ; i++) {	/* create subBuckets */
		new_bucket->key_buckets[i].key = 0;
		new_bucket->key_buckets[i].current_entries = 0;
		new_bucket->key_buckets[i].transaction_range = malloc(C * sizeof(RangeArray));
		ALLOCATION_ERROR(new_bucket->key_buckets[i].transaction_range);
		for (j = 0 ; j < C ; j++) {		/* create transaction Range */
			new_bucket->key_buckets[i].transaction_range[j].transaction_id = 0;
			new_bucket->key_buckets[i].transaction_range[j].rec_offset = 0;
		}
	}
	return new_bucket;
}

/* Adds the Key that caused the conflict as the last element of the temp_bucket transactions array */
void addNewKeyToTmpBucket(Bucket *tmp_bucket, Key key, RangeArray* rangeArray) {
	tmp_bucket->key_buckets[B].key = key;
	tmp_bucket->key_buckets[B].transaction_range[0].transaction_id = rangeArray->transaction_id;
	tmp_bucket->key_buckets[B].transaction_range[0].rec_offset = rangeArray->rec_offset;
	tmp_bucket->key_buckets[B].current_entries++;
}

/* The conflict bucket is empty of transaction, just hold the local_depth */
void cleanBucket(Bucket* conflict_bucket) {
	uint32_t i;
	uint64_t j;
	for (i = 0 ; i < conflict_bucket->current_subBuckets ; i++) {					/* for i in all subBuckets */
		conflict_bucket->key_buckets[i].key = 0;
		conflict_bucket->key_buckets[i].current_entries = 0;
		for (j = 0 ; j < conflict_bucket->key_buckets[i].current_entries ; j++) {	/* for j in transaction range */
			conflict_bucket->key_buckets[i].transaction_range[j].transaction_id = 0;
			conflict_bucket->key_buckets[i].transaction_range[j].rec_offset = 0;
		}
	}
	conflict_bucket->current_subBuckets = 0;
}

uint64_t hashFunction(uint64_t size, uint64_t n) {
	return n % size;
}

/* printsBucket various info */
void printBucket(Bucket* bucket){
	fprintf(stderr, "------------------------------------------------------------\n");
	fprintf(stderr, "local_depth(%"PRIu32"), current_subBuckets(%"PRIu32")\n", bucket->local_depth, bucket->current_subBuckets);
	uint64_t i;
	uint32_t j;
	for (i = 0 ; i < bucket->current_subBuckets ; i++) { //
		Key key = bucket->key_buckets[i].key;
		uint64_t current_entries = bucket->key_buckets[i].current_entries;
		fprintf(stderr, "\tSubBucket(%zd): key: %zd, current_entries: %zd\n", i, key, current_entries);
		for (j = 0 ; j < current_entries ; j++) {
			uint64_t tid = bucket->key_buckets[i].transaction_range[j].transaction_id;
			fprintf(stderr, "\t\ttid: %zd\n", tid);
		}
	}
	fprintf(stderr, "------------------------------------------------------------\n");
}

/* Prints index, the bucket pointing to and the content of the bucket */
void printHash(Hash* hash){
	uint64_t i;
	for (i = 0 ; i < hash->size ; i++) { /*for every index*/
		if (hash->index[i] != NULL) { /*points somewhere*/
			fprintf(stderr, "*****Index %zd points to Bucket address %p*******\n", i, hash->index[i]);
			if (!hash->index[i]->current_subBuckets) {
				printBucket(hash->index[i]);
			} else {
				printBucket(hash->index[i]);
			}
			fprintf(stderr,"***********************************************************\n\n");
		} else {
			fprintf(stderr,"~~~~~Index (%zd) points to no bucket~~~~~\n", i);
		}
	}
}



/*
// //  * We use it on Recursion. This function recreates the conflict_bucket (remember it was empty from
// //  * the cleanConflictBucket) and destroys the malloced tmp_bucket. After this function call, comes 
// //  * the recursion on spliBucket
// // */
// void destroyTempBucketRecreateConflict(Bucket * conflict_bucket,Bucket *tmp_bucket) {
// 	int i;
// 	for (i = 0 ; i < C ; i++) {
// 		conflict_bucket->transaction_range[i].transaction_id = tmp_bucket->transaction_range[i].transaction_id;
// 		conflict_bucket->transaction_range[i].rec = tmp_bucket->transaction_range[i].rec;
// 	}
// 	conflict_bucket->capacity = C;
// 	conflict_bucket->current_entries = C;
// 	conflict_bucket->local_depth = tmp_bucket->local_depth;
// 	free(tmp_bucket->transaction_range);
// 	free(tmp_bucket);
// 	tmp_bucket = NULL;
// }

// RangeArray* getHashRecord(Hash* hash, Key key) {
// 	uint64_t bucket_num = hashFunction(hash->size, key);
// 	return hash->index[bucket_num]->transaction_range;
// } 

// JournalRecord_t* getHashRecord2(Hash* hash, Key key) {
// 	uint64_t bucket_num = hashFunction(hash->size, key);
// 	return searchIndexByKey(hash,bucket_num,key);
// }

//  Binary Search for first appearance 
// JournalRecord_t* searchIndexByKey(Hash* hash,uint64_t bucket_num,uint64_t keyToSearch) {
// 	Bucket *bucket = hash->index[bucket_num];
// 	int i;
// 	for (i = 0 ; i < bucket->current_entries ; i++) {
// 		if (bucket->transaction_range[i].rec->column_values[0] == keyToSearch) {
// 			return bucket->transaction_range[i].rec;
// 		}
// 	}
// 	return NULL;
// }

// int destroyHash(Hash* hash) {
// 	uint64_t i, j;
// 	fprintf(stderr, "deleting: size is %zd\n", hash->size);
// 	for (i = 0 ; i < hash->size ; i++) {	/* for all buckets */
// 		if (hash->index[i] != NULL) {	/* if is not already freed */
// 			if (hash->index[i]->transaction_range != NULL) {
// 				for (j = 0 ; j < hash->index[i]->current_entries ; j++) {
// 					hash->index[i]->transaction_range[j].rec = NULL;
// 				}
// 				free(hash->index[i]->transaction_range);
// 				hash->index[i]->transaction_range = NULL;
// 			}
// 			/*
// 			* oi 2 apo katw grammes exoun to provlhma, otan tis 3esxoliazw vgazei errors to valgrind
// 			* profanws to error den einai sto free
// 			*/
// 			// free(hash->index[i]);
// 			// hash->index[i] = NULL;
// 		}
// 	}
// 	free(hash);
// } 
