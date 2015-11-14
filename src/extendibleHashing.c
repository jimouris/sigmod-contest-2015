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

int insertHashRecord(Hash* hash, Key key, RangeArray* rangeArray) {
	uint64_t bucket_num = hashFunction(hash->size, key);
	Bucket *bucket = hash->index[bucket_num];
	uint32_t i;
	for (i = 0 ; i < bucket->current_subBuckets; i++) {			/* for all subbuckets */
		if (bucket->key_buckets[i].key == key) {				/* if there is a subBucket with this key */
			uint64_t current_entries = bucket->key_buckets[i].current_entries;
			if (current_entries == bucket->key_buckets[i].limit) { 	/* if there isn't enough free space, realloc */
				bucket->key_buckets[i].transaction_range = realloc(bucket->key_buckets[i].transaction_range, ((bucket->key_buckets[i].current_entries)+C) * sizeof(RangeArray));
				ALLOCATION_ERROR(bucket->key_buckets[i].transaction_range);
				bucket->key_buckets[i].limit += C;
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
			doublicateIndex(hash);	/* doublicates and increases global depth */
			bucket->local_depth++;
			Bucket *new_bucket = createNewBucket(hash->index[bucket_num]->local_depth, B);
			Bucket *tmp_bucket = createNewBucket(hash->index[bucket_num]->local_depth, B);
			copyBucketTransactions(tmp_bucket, hash->index[bucket_num]);
			cleanBucket(hash->index[bucket_num]);
			fixHashPointers(hash->index, new_bucket, hash->global_depth, bucket_num);
			uint32_t i;
			uint64_t new_hash, j;
			for (i = 0 ; i < B ; i++) {	/* for all subBuckets in tmpBucket */
				new_hash = hashFunction(1 << hash->global_depth, tmp_bucket->key_buckets[i].key); /* new_hash has a bucket number */
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
				}
			}
			insertHashRecord(hash, key, rangeArray);
		} 
		/* split bucket */
		else if (bucket->local_depth < hash->global_depth) { 
			bucket->local_depth++;
			Bucket *new_bucket = createNewBucket(hash->index[bucket_num]->local_depth, B);
			Bucket *tmp_bucket = createNewBucket(hash->index[bucket_num]->local_depth, B);
			copyBucketTransactions(tmp_bucket, hash->index[bucket_num]);
			cleanBucket(hash->index[bucket_num]);
			uint32_t i;
			uint64_t new_hash, j;
			int flag = 0;
			for (i = 0 ; i < B ; i++) {	/* for all subBuckets in tmpBucket */
				new_hash = hashFunction(1 << hash->global_depth, tmp_bucket->key_buckets[i].key); /* new_hash has a bucket number */
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
				} else if (new_hash != bucket_num && new_bucket->current_subBuckets < B) {
					uint32_t current_subBuckets = new_bucket->current_subBuckets;
					new_bucket->key_buckets[current_subBuckets].key = tmp_bucket->key_buckets[i].key;
					new_bucket->key_buckets[current_subBuckets].current_entries = tmp_bucket->key_buckets[i].current_entries;
					uint64_t current_entries = new_bucket->key_buckets[current_subBuckets].current_entries;
					for (j = 0 ; j < current_entries ; j++ ) {
						new_bucket->key_buckets[current_subBuckets].transaction_range[j].transaction_id = tmp_bucket->key_buckets[i].transaction_range[j].transaction_id;
						new_bucket->key_buckets[current_subBuckets].transaction_range[j].rec_offset = tmp_bucket->key_buckets[i].transaction_range[j].rec_offset;
					}
					new_bucket->current_subBuckets++;
					flag = 1;
				}
			}
			if (flag) {
				hash->index[new_hash] = new_bucket;
			} else {
				destroyBucket(new_bucket, B);
			} 
			insertHashRecord(hash, key, rangeArray);
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


void destroyBucket(Bucket *bucket,uint32_t b) {
	uint32_t i;
	for (i = 0 ; i < b ; i++) { /*for each subBucket*/
		free(bucket->key_buckets[i].transaction_range);
	}
	free(bucket->key_buckets);
	free(bucket);
	bucket = NULL;
}

/* fix new indexes pointers after index doublicate or just splits pointers */
void fixHashPointers(Bucket **index, Bucket *new_bucket, uint32_t global_depth, uint64_t bucket_num) {
	int i, j;
	uint64_t old_size = 1 << (global_depth-1);
	for (i = 0, j = old_size ; i < old_size ; i++, j++) {
		if (i == bucket_num){
			index[j] = new_bucket;
		}
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
		new_bucket->key_buckets[i].limit = C;
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

uint64_t hashFunction(uint64_t size, uint64_t x) {
	// x = ((x >> 16) ^ x) * 0x45d9f3b;
 //    x = ((x >> 16) ^ x) * 0x45d9f3b;
 //    x = ((x >> 16) ^ x);
    return (x % size);
	// return ((n*2654435761+1223) % size);
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
			uint64_t rec_offset = bucket->key_buckets[i].transaction_range[j].rec_offset;
			fprintf(stderr, "\t\ttid: %zd, offset :%zd\n", tid, rec_offset);
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

RangeArray* getHashRecord(Hash* hash, Key key, uint64_t * current_entries) {
	uint64_t bucket_num = hashFunction(hash->size, key);
	uint32_t i;
	/* note: May need binary search later */
	for (i = 0 ; i < hash->index[bucket_num]->current_subBuckets ; i++) { /* for i in subBuckets */
		if (hash->index[bucket_num]->key_buckets[i].key == key) {
			*current_entries = hash->index[bucket_num]->key_buckets[i].current_entries;
			return hash->index[bucket_num]->key_buckets[i].transaction_range;
		}
	}
	return NULL;
} 

JournalRecord_t* getLastRecord(Journal_t* journal, Key key) {
	Hash* hash = journal->index;
	uint64_t current_entries;
	RangeArray* range = getHashRecord(hash, key, &current_entries);
	if(range == NULL){
		return NULL;
	}
	return &journal->records[range[current_entries-1].rec_offset];
}

int destroyHash(Hash* hash) {
	uint64_t i;
	for (i = 0 ; i < hash->size ; i++) { /*for every bucket on the hash*/
		uint32_t j;
		Bucket * bucketPtr = hash->index[i];
		if (hash->index[i] != NULL) {

			for (j = 0 ; j < bucketPtr->current_subBuckets ; j++) {
				free(bucketPtr->key_buckets[j].transaction_range);
			}
			free(bucketPtr->key_buckets);
			uint32_t k;
			for (k = i ; k < hash->size ; k++) {
				if (hash->index[k] == bucketPtr) {
					hash->index[k] = NULL ;
				}
			}
			free(bucketPtr);
			bucketPtr = NULL;
			hash->index[i] = NULL;
		}
	}
	free(hash->index);
	free(hash);
	hash = NULL;
	return OK_SUCCESS;
}

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
