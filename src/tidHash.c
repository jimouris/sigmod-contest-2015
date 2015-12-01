#include "tidHash.h"

tidHash* tidCreateHash() {
	tidHash *hash = malloc(sizeof(tidHash));
	ALLOCATION_ERROR(hash);
	hash->global_depth = TID_GLOBAL_DEPTH_INIT;
	hash->size = 1 << TID_GLOBAL_DEPTH_INIT;
	hash->index = malloc(hash->size * sizeof(tidBucket *));
	ALLOCATION_ERROR(hash->index);
	uint64_t i;
	for (i = 0 ; i < hash->size ; i++)
		hash->index[i] = tidCreateNewBucket(TID_GLOBAL_DEPTH_INIT);
	return hash;
}

// int insertHashRecord(tidHash* hash, Key key, RangeArray* rangeArray) {
// 	uint64_t bucket_num = hashFunction(hash->size, key);
// 	tidBucket *bucket = hash->index[bucket_num];
// 	uint32_t i;
// 	for (i = 0 ; i < bucket->current_subBuckets; i++) {			/* for all subbuckets */
// 		if (bucket->key_buckets[i].key == key) {				/* if there is a tidSubBucket with this key */
// 			uint64_t current_entries = bucket->key_buckets[i].current_entries;
// 			if (current_entries == bucket->key_buckets[i].limit) { 	/* if there isn't enough free space, realloc */
// 				bucket->key_buckets[i].transaction_range = realloc(bucket->key_buckets[i].transaction_range, ((bucket->key_buckets[i].current_entries)+C) * sizeof(RangeArray));
// 				ALLOCATION_ERROR(bucket->key_buckets[i].transaction_range);
// 				bucket->key_buckets[i].limit += C;
// 			}
// 			/* Insert entry */
// 			bucket->key_buckets[i].transaction_range[current_entries].transaction_id = rangeArray->transaction_id;
// 			bucket->key_buckets[i].transaction_range[current_entries].rec_offset = rangeArray->rec_offset;
// 			(bucket->key_buckets[i].current_entries)++;
// 			return OK_SUCCESS;
// 		}
// 	}
// 	/* If that was the first appearence of this key */
// 	if (bucket->current_subBuckets < TID_B) {	/* If there is space to insert it on that bucket (there is a free subbucket) */
// 		bucket->key_buckets[bucket->current_subBuckets].key = key;
// 		bucket->key_buckets[bucket->current_subBuckets].transaction_range[0].transaction_id = rangeArray->transaction_id;
// 		bucket->key_buckets[bucket->current_subBuckets].transaction_range[0].rec_offset = rangeArray->rec_offset;
// 		bucket->key_buckets[bucket->current_subBuckets].current_entries += 1;
// 		(bucket->current_subBuckets)++;
// 		return OK_SUCCESS;
// 	} else {
// 		bucket->local_depth++;
// 		tidBucket *new_bucket = createNewBucket(bucket->local_depth);
// 		tidBucket *tmp_bucket = createNewBucket(bucket->local_depth);
// 		copyBucketTransactions(tmp_bucket, bucket);
// 		if (bucket->local_depth-1 >= hash->global_depth) { /* duplicate case */
// 			duplicateIndex(hash);	/* duplicates and increases global depth */
// 			fixHashPointers(hash->index, new_bucket, hash->global_depth, bucket_num);
// 		} else if (bucket->local_depth-1 < hash->global_depth) { /* split case*/
// 			fixSplitPointers(hash, bucket, new_bucket, bucket_num);
// 		}
// 		cleanBucket(bucket);
// 		uint32_t i;
// 		uint64_t new_hash;
// 		for (i = 0 ; i < TID_B ; i++) {	/* for all subBuckets in tmpBucket */
// 			tidSubBucket *subbuckets = tmp_bucket->key_buckets;
// 			Key sub_key = subbuckets[i].key;
// 			new_hash = hashFunction(hash->size, sub_key);
// 			// fprintf(stderr, "New hash for %zu is %zu\n",key,new_hash );
// 			tidBucket* destination = hash->index[new_hash];
// 			uint32_t current_subBuckets = destination->current_subBuckets;
// 			copySubbucketTransactions(&destination->key_buckets[current_subBuckets], &subbuckets[i]);
// 			destination->current_subBuckets++;
// 		}
// 		destroyBucket(tmp_bucket);
// 		insertHashRecord(hash, key, rangeArray);
// 	}
// 	return OK_SUCCESS;
// }  

// /*Does whatever it says*/
void tidDuplicateIndex(tidHash * hash) {
	hash->global_depth++;
	uint64_t old_size = hash->size;
	hash->size *= 2;
	hash->index = realloc(hash->index, hash->size * sizeof(tidBucket *));
	ALLOCATION_ERROR(hash->index);
	uint64_t i;
	for (i = old_size; i < hash->size; i++)
		hash->index[i] = NULL;
}

// void destroyBucket(tidBucket *bucket) {
// 	uint32_t i;
// 	for (i = 0 ; i < TID_B ; i++) { /*for each tidSubBucket*/
// 		free(bucket->key_buckets[i].transaction_range);
// 	}
// 	free(bucket->key_buckets);
// 	free(bucket);
// 	bucket = NULL;
// }

// /* fix new indexes pointers after index doublicate */
// void fixHashPointers(tidBucket **index, tidBucket *new_bucket, uint32_t global_depth, uint64_t bucket_num) {
// 	uint64_t i, j;
// 	uint64_t old_size = 1 << (global_depth-1);
// 	for (i = 0, j = old_size ; i < old_size ; i++, j++) {
// 		if (i == bucket_num) {
// 			index[j] = new_bucket;
// 		} else {
// 			index[j] = index[i];
// 		}
// 	}
// }

// /* fix new indexes pointers after index splits */
// void fixSplitPointers(tidHash* hash, tidBucket* old_bucket, tidBucket* new_bucket, uint64_t bucket_num){
// 	uint64_t prev_local_depth = old_bucket->local_depth-1;
// 	uint64_t i;
// 	uint64_t ld_size = 1 << prev_local_depth;
// 	uint32_t first_pointer = bucket_num % ld_size;
// 	for (i = first_pointer ; i < hash->size ; i+=(2*ld_size)) { /* i in all old_bucket pointers */
// 		hash->index[i] = old_bucket;
// 		hash->index[i+ld_size] = new_bucket;
// 	}
// }

// void fixDeletePointers(tidHash* hash, tidBucket* bucket, tidBucket* buddyBucket, uint64_t buddy_index) {
// 	uint64_t i;
// 	uint64_t ld_size = 1 << buddyBucket->local_depth;
// 	uint32_t first_pointer = buddy_index % ld_size;
// 	for (i = first_pointer ; i < hash->size ; i += ld_size ) {
// 		hash->index[i] = bucket;
// 	}
// }


// /* copy transactions from one tidBucket to another one*/
// void copyBucketTransactions(tidBucket* dst, tidBucket* src) {
// 	uint64_t i;
// 	dst->local_depth = src->local_depth;
// 	dst->current_subBuckets = src->current_subBuckets;

// 	for (i = 0 ; i < src->current_subBuckets ; i++) {	/* for i in subBuckets */
// 		copySubbucketTransactions(&dst->key_buckets[i], &src->key_buckets[i]);
// 	}
// }

// void copySubbucketTransactions(tidSubBucket* dst, tidSubBucket* src){
// 	uint64_t j;
// 	dst->key = src->key;
// 	dst->current_entries = src->current_entries;

// 	if (dst->current_entries > dst->limit) {
// 		dst->transaction_range = realloc(dst->transaction_range, src->limit*sizeof(RangeArray));
// 		ALLOCATION_ERROR(dst->transaction_range);
// 	}
// 	dst->limit = src->limit;
// 	for (j = 0 ; j < src->current_entries ; j++) { /* for j in transactionRange */
// 		dst->transaction_range[j].transaction_id = src->transaction_range[j].transaction_id;			
// 		dst->transaction_range[j].rec_offset = src->transaction_range[j].rec_offset;
// 	}
// }

/* creates an empty tidBucket*/
tidBucket* tidCreateNewBucket(uint32_t local_depth) {
	tidBucket *new_bucket = malloc(sizeof(tidBucket));
	ALLOCATION_ERROR(new_bucket);
	new_bucket->local_depth = local_depth;
	new_bucket->current_subBuckets = 0;
	new_bucket->deletion_started = 0;
	new_bucket->key_buckets = malloc(TID_B * sizeof(tidSubBucket));
	ALLOCATION_ERROR(new_bucket->key_buckets);
	uint32_t i;
	for (i = 0 ; i < TID_B ; i++) {	/* create subBuckets */
		new_bucket->key_buckets[i].transaction_id = 0;
		new_bucket->key_buckets[i].rec_offset = 0;
	}
	return new_bucket;
}

// /* Adds the Key that caused the conflict as the last element of the temp_bucket transactions array */
// void addNewKeyToTmpBucket(tidBucket *tmp_bucket, Key key, RangeArray* rangeArray) {
// 	tmp_bucket->key_buckets[TID_B].key = key;
// 	tmp_bucket->key_buckets[TID_B].transaction_range[0].transaction_id = rangeArray->transaction_id;
// 	tmp_bucket->key_buckets[TID_B].transaction_range[0].rec_offset = rangeArray->rec_offset;
// 	tmp_bucket->key_buckets[TID_B].current_entries++;
// }

// /* The conflict bucket is empty of transaction, just hold the local_depth */
// void cleanBucket(tidBucket* conflict_bucket) {
// 	uint32_t i;
// 	for (i = 0 ; i < conflict_bucket->current_subBuckets ; i++) {	 for i in all subBuckets 
// 		cleanSubBucket(&conflict_bucket->key_buckets[i]);
// 	}
// 	conflict_bucket->current_subBuckets = 0;
// }

// void cleanSubBucket(tidSubBucket* tidSubBucket) {
// 	uint64_t j,iter = tidSubBucket->current_entries; /* reduce iterations */
// 	tidSubBucket->key = 0;
// 	if (tidSubBucket->current_entries > C ) { /* we must realloc to the default capacity C */
// 		tidSubBucket->transaction_range = realloc(tidSubBucket->transaction_range,C * sizeof(RangeArray));
// 		ALLOCATION_ERROR(tidSubBucket->transaction_range);
// 		tidSubBucket->limit = C;
// 		iter = C;
// 	}
// 	for (j = 0 ; j < iter ; j++) {	/* for j in transaction range */
// 		tidSubBucket->transaction_range[j].transaction_id = 0;
// 		tidSubBucket->transaction_range[j].rec_offset = 0;
// 	}
// 	tidSubBucket->current_entries = 0;
// }

// uint64_t hashFunction(uint64_t size, uint64_t x) {
//     return (x % size);
//     // x = ((x >> 16) ^ x) * 0x45d9f3b;
// 	//    x = ((x >> 16) ^ x) * 0x45d9f3b;
// 	//    x = ((x >> 16) ^ x);
// 	// return ((x*2654435761+1223) % size);
// }

// /* printsBucket various info */
// void printBucket(tidBucket* bucket){
// 	fprintf(stderr, "------------------------------------------------------------\n");
// 	fprintf(stderr, "local_depth(%"PRIu32"), current_subBuckets(%"PRIu32")\n", bucket->local_depth, bucket->current_subBuckets);
// 	uint64_t i;
// 	uint32_t j;
// 	for (i = 0 ; i < bucket->current_subBuckets ; i++) { //
// 		Key key = bucket->key_buckets[i].key;
// 		uint64_t current_entries = bucket->key_buckets[i].current_entries;
// 		fprintf(stderr, "\tSubBucket(%zd): key: %zd, current_entries: %zd\n", i, key, current_entries);
// 		for (j = 0 ; j < current_entries ; j++) {
// 			uint64_t tid = bucket->key_buckets[i].transaction_range[j].transaction_id;
// 			uint64_t rec_offset = bucket->key_buckets[i].transaction_range[j].rec_offset;
// 			fprintf(stderr, "\t\ttid: %zd, offset :%zd\n", tid, rec_offset);
// 		}
// 	}
// 	fprintf(stderr, "------------------------------------------------------------\n");
// }

// /* Prints index, the bucket pointing to and the content of the bucket */
// void printHash(tidHash* hash){
// 	uint64_t i;
// 	for (i = 0 ; i < hash->size ; i++) { /*for every index*/
// 		if (hash->index[i] != NULL) { /*points somewhere*/
// 			fprintf(stderr, "*****Index %zd points to tidBucket address %p*******\n", i, hash->index[i]);
// 			if (!hash->index[i]->current_subBuckets) {
// 				printBucket(hash->index[i]);
// 			} else {
// 				printBucket(hash->index[i]);
// 			}
// 			fprintf(stderr,"***********************************************************\n\n");
// 		} else {
// 			fprintf(stderr,"~~~~~Index (%zd) points to no bucket~~~~~\n", i);
// 		}
// 	}
// }

// RangeArray* getHashRecord(tidHash* hash, Key key, uint64_t * current_entries) {
// 	uint64_t bucket_num = hashFunction(hash->size, key);
// 	uint32_t i;
// 	/* note: May need binary search later */
// 	for (i = 0 ; i < hash->index[bucket_num]->current_subBuckets ; i++) { /* for i in subBuckets */
// 		if (hash->index[bucket_num]->key_buckets[i].key == key) {
// 			*current_entries = hash->index[bucket_num]->key_buckets[i].current_entries;
// 			return hash->index[bucket_num]->key_buckets[i].transaction_range;
// 		}
// 	}
// 	*current_entries = 0;
// 	return NULL;
// } 

// JournalRecord_t* getLastRecord(Journal_t* journal, Key key) {
// 	tidHash* hash = journal->index;
// 	uint64_t current_entries;
// 	RangeArray* range = getHashRecord(hash, key, &current_entries);
// 	if(range == NULL){
// 		return NULL;
// 	}
// 	return &journal->records[range[current_entries-1].rec_offset];
// }

int tidDestroyHash(tidHash* hash) {
	uint64_t i;
	for (i = 0 ; i < hash->size ; i++) { /*for every bucket on the hash*/
		tidBucket * bucketPtr = hash->index[i];
		if (!bucketPtr->deletion_started) { /*it is the first bucket*/
			bucketPtr->deletion_started = 1;
			bucketPtr->pointers_num = 1 << (hash->global_depth - bucketPtr->local_depth);
		}

		if (bucketPtr->pointers_num == 1 ) { /*if it is the last remaining pointer that points to the bucket*/
			free(bucketPtr->key_buckets);
			free(bucketPtr);
		}else{
			bucketPtr->pointers_num--;
		}
		hash->index[i] = NULL;
		bucketPtr = NULL;
	}
	free(hash->index);
	free(hash);
	hash = NULL;
	return OK_SUCCESS;
}

// int deleteHashRecord(tidHash* hash, Key key) {
// 	uint64_t bucket_num = hashFunction(hash->size, key);
// 	// tidBucket *bucket = hash->index[bucket_num];
// 	int deletion_found = deleteSubBucket(hash,bucket_num,key);
// 	if (!deletion_found)
// 		return 0;
// 	return 1;
// }

// void moveSubBucketsLeft(tidBucket* bucket, uint32_t index) {
// 		uint32_t i;
// 		for(i = index ; i < bucket->current_subBuckets; i++) {
// 			copySubbucketTransactions(&bucket->key_buckets[i-1],&bucket->key_buckets[i]);
// 			cleanSubBucket(&bucket->key_buckets[i]);
// 	}
// }

// int deleteSubBucket(tidHash* hash, uint64_t bucket_num, Key key) {
// 	tidBucket *bucket = hash->index[bucket_num];
// 	uint32_t i;
// 	for (i = 0 ; i < bucket->current_subBuckets; i++) {	/* for all subbuckets */
// 		if (bucket->key_buckets[i].key == key) {	/* tidSubBucket for deletion found*/
// 			/* physical remove of the SubBukcet that contains the Key */
// 			cleanSubBucket(&bucket->key_buckets[i]);
// 			moveSubBucketsLeft(bucket,i+1);
// 			bucket->current_subBuckets--;
// 			/*now try to merge Buckets*/
// 			tryMergeBuckets(hash,bucket_num);
// 			return 1;
// 		}
// 	}
// 	return 0;
// }

// unsigned char tryCollapseIndex(tidHash* hash) {
// 	unsigned char canCollapse = 0;
// 	if (hash->size > 1) {
// 		canCollapse = 1;
// 		uint64_t i,j;
// 		for (i = 0,j=hash->size/2; i < hash->size/2; i++,j++) {
// 			if (hash->index[i] != hash->index[j]) { /*we can not doublicate*/
// 				canCollapse = 0;
// 				break;
// 			}
// 		}
// 	}
// 	if (canCollapse) {
// 		hash->global_depth --;
// 		hash->size /= 2;
// 		hash->index = realloc(hash->index,hash->size * sizeof(tidBucket *));
// 		ALLOCATION_ERROR(hash->index);
// 	}
// 	return canCollapse;
// }

// void tryMergeBuckets(tidHash* hash, uint64_t bucket_num ) {
// 	tidBucket *bucket = hash->index[bucket_num];
// 	uint32_t ld_oldSize = 1 << (bucket->local_depth - 1);
// 	uint64_t buddy_index;
// 	/*find its buddy bucket and store its index to buddy_index*/
// 	if (bucket_num + ld_oldSize < hash->size) { /*buddy bucket is under*/
// 		buddy_index = bucket_num + ld_oldSize;
// 		// fprintf(stderr,"It is under with buddy_index %llu\n",buddy_index);
// 	} else { /*buddy bucket is above*/
// 		buddy_index = bucket_num - ld_oldSize;
// 		// fprintf(stderr,"It is above with buddy_index %llu\n",buddy_index);
// 	}

// 	if (bucket != hash->index[buddy_index] && bucket->local_depth == hash->index[buddy_index]->local_depth) { /*we have a buddy and not the the bucket itself*/
// 		tidBucket *buddyBucket = hash->index[buddy_index];
// 		uint64_t mergedBucket_entries = bucket->current_subBuckets + buddyBucket->current_subBuckets;
// 		if (mergedBucket_entries <= TID_B) { /*we can merge the two subBuckets*/
// 			fprintf(stderr,"tidBucket(%zu) - Buddy(%zu)\n",bucket_num,buddy_index);
// 			uint64_t i,j;
// 			for (i = bucket->current_subBuckets,j=0; i < mergedBucket_entries ; i++,j++) {
// 				copySubbucketTransactions(&bucket->key_buckets[i],&buddyBucket->key_buckets[j]);
// 			}
// 			fixDeletePointers(hash, bucket, buddyBucket, buddy_index);
// 			bucket->current_subBuckets = mergedBucket_entries;
// 			bucket->local_depth--;
// 			destroyBucket(buddyBucket);
// 			if (tryCollapseIndex(hash)) {
// 				tryMergeBuckets(hash, bucket_num % (1 << bucket->local_depth));
// 			}
// 		}
// 	}

// }

//  Binary Search for first appearance 
// JournalRecord_t* searchIndexByKey(tidHash* hash,uint64_t bucket_num,uint64_t keyToSearch) {
// 	tidBucket *bucket = hash->index[bucket_num];
// 	int i;
// 	for (i = 0 ; i < bucket->current_entries ; i++) {
// 		if (bucket->transaction_range[i].rec->column_values[0] == keyToSearch) {
// 			return bucket->transaction_range[i].rec;
// 		}
// 	}
// 	return NULL;
// }
