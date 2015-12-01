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

int tidInsertHashRecord(tidHash* hash, tidSubBucket* tid_record) {
	uint64_t bucket_num = tidHashFunction(hash->size, tid_record->transaction_id);
	tidBucket *bucket = hash->index[bucket_num];
	uint32_t i;
	for (i = 0 ; i < bucket->current_subBuckets; i++) {			/* for all subbuckets */
		if (bucket->key_buckets[i].transaction_id == tid_record->transaction_id) {	/* if there is a tidSubBucket with this transaction_id */
			return OK_SUCCESS;
		}
	}
	/* If that was the first appearence of this key */
	if (bucket->current_subBuckets < TID_B) {	/* If there is space to insert it on that bucket (there is a free subbucket) */
		bucket->key_buckets[bucket->current_subBuckets].transaction_id = tid_record->transaction_id;
		bucket->key_buckets[bucket->current_subBuckets].rec_offset = tid_record->rec_offset;
		(bucket->current_subBuckets)++;
		return OK_SUCCESS;
	} else {
		bucket->local_depth++;
		tidBucket *new_bucket = tidCreateNewBucket(bucket->local_depth);
		tidBucket *tmp_bucket = tidCreateNewBucket(bucket->local_depth);
		tidCopyBucketTransactions(tmp_bucket, bucket);
		if (bucket->local_depth-1 >= hash->global_depth) { /* duplicate case */
			tidDuplicateIndex(hash);	/* duplicates and increases global depth */
			tidFixHashPointers(hash->index, new_bucket, hash->global_depth, bucket_num);
		} else if (bucket->local_depth-1 < hash->global_depth) { /* split case*/
			tidFixSplitPointers(hash, bucket, new_bucket, bucket_num);
		}
		tidCleanBucket(bucket);
		uint32_t i;
		uint64_t new_hash;
		for (i = 0 ; i < TID_B ; i++) {	/* for all subBuckets in tmpBucket */
			tidSubBucket *subbuckets = tmp_bucket->key_buckets;
			uint64_t sub_transaction_id = subbuckets[i].transaction_id;
			new_hash = tidHashFunction(hash->size, sub_transaction_id);
			// fprintf(stderr, "New hash for %zu is %zu\n",key,new_hash );
			tidBucket* destination = hash->index[new_hash];
			uint32_t current_subBuckets = destination->current_subBuckets;
			tidCopySubbucketTransactions(&destination->key_buckets[current_subBuckets], &subbuckets[i]);
			destination->current_subBuckets++;
		}
		tidDestroyBucket(tmp_bucket);
		tidInsertHashRecord(hash, tid_record);
	}
	return OK_SUCCESS;
}  

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

void tidDestroyBucket(tidBucket *bucket) {
	free(bucket->key_buckets);
	free(bucket);
	bucket = NULL;
}

// /* fix new indexes pointers after index doublicate */
void tidFixHashPointers(tidBucket **index, tidBucket *new_bucket, uint32_t global_depth, uint64_t bucket_num) {
	uint64_t i, j;
	uint64_t old_size = 1 << (global_depth-1);
	for (i = 0, j = old_size ; i < old_size ; i++, j++) {
		if (i == bucket_num) {
			index[j] = new_bucket;
		} else {
			index[j] = index[i];
		}
	}
}

// /* fix new indexes pointers after index splits */
void tidFixSplitPointers(tidHash* hash, tidBucket* old_bucket, tidBucket* new_bucket, uint64_t bucket_num){
	uint64_t prev_local_depth = old_bucket->local_depth-1;
	uint64_t i;
	uint64_t ld_size = 1 << prev_local_depth;
	uint32_t first_pointer = bucket_num % ld_size;
	for (i = first_pointer ; i < hash->size ; i+=(2*ld_size)) { /* i in all old_bucket pointers */
		hash->index[i] = old_bucket;
		hash->index[i+ld_size] = new_bucket;
	}
}

// void fixDeletePointers(tidHash* hash, tidBucket* bucket, tidBucket* buddyBucket, uint64_t buddy_index) {
// 	uint64_t i;
// 	uint64_t ld_size = 1 << buddyBucket->local_depth;
// 	uint32_t first_pointer = buddy_index % ld_size;
// 	for (i = first_pointer ; i < hash->size ; i += ld_size ) {
// 		hash->index[i] = bucket;
// 	}
// }


// /* copy transactions from one tidBucket to another one*/
void tidCopyBucketTransactions(tidBucket* dst, tidBucket* src) {
	uint64_t i;
	dst->local_depth = src->local_depth;
	dst->current_subBuckets = src->current_subBuckets;
	for (i = 0 ; i < src->current_subBuckets ; i++) {	/* for i in subBuckets */
		tidCopySubbucketTransactions(&dst->key_buckets[i], &src->key_buckets[i]);
	}
}

void tidCopySubbucketTransactions(tidSubBucket* dst, tidSubBucket* src){
	dst->transaction_id = src->transaction_id;
	dst->rec_offset = src->rec_offset;
}

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

// /* The conflict bucket is empty of transaction, just hold the local_depth */
void tidCleanBucket(tidBucket* conflict_bucket) {
	uint32_t i;
	for (i = 0 ; i < conflict_bucket->current_subBuckets ; i++) {	/* for i in all subBuckets*/ 
		tidCleanSubBucket(&conflict_bucket->key_buckets[i]);
	}
	conflict_bucket->current_subBuckets = 0;
}

void tidCleanSubBucket(tidSubBucket* tidSubBucket) {
	tidSubBucket->transaction_id = 0;
	tidSubBucket->rec_offset = 0;
}

uint64_t tidHashFunction(uint64_t size, uint64_t x) {
    return (x % size);
//     // x = ((x >> 16) ^ x) * 0x45d9f3b;
// 	//    x = ((x >> 16) ^ x) * 0x45d9f3b;
// 	//    x = ((x >> 16) ^ x);
// 	// return ((x*2654435761+1223) % size);
 }

// /* printsBucket various info */
void tidPrintBucket(tidBucket* bucket){
	fprintf(stderr, "------------------------------------------------------------\n");
	fprintf(stderr, "local_depth(%"PRIu32"), current_subBuckets(%"PRIu32")\n", bucket->local_depth, bucket->current_subBuckets);
	uint64_t i;
	for (i = 0 ; i < bucket->current_subBuckets ; i++) { 
		uint64_t transaction_id = bucket->key_buckets[i].transaction_id;
		uint64_t rec_offset = bucket->key_buckets[i].rec_offset;
		fprintf(stderr, "\tSubBucket(%zd): transaction_id: %zd , rec_offset : %zd\n", i, transaction_id,rec_offset);
	}
	fprintf(stderr, "------------------------------------------------------------\n");
}

// /* Prints index, the bucket pointing to and the content of the bucket */
void tidPrintHash(tidHash* hash){
	uint64_t i;
	for (i = 0 ; i < hash->size ; i++) { /*for every index*/
		if (hash->index[i] != NULL) { /*points somewhere*/
			fprintf(stderr, "*****Index %zd points to tidBucket address %p*******\n", i, hash->index[i]);
			if (!hash->index[i]->current_subBuckets) {
				tidPrintBucket(hash->index[i]);
			} else {
				tidPrintBucket(hash->index[i]);
			}
			fprintf(stderr,"***********************************************************\n\n");
		} else {
			fprintf(stderr,"~~~~~Index (%zd) points to no bucket~~~~~\n", i);
		}
	}
}

uint64_t tidGetHashOffset(tidHash* hash, uint64_t transaction_id, Boolean_t *found) {
	uint64_t bucket_num = tidHashFunction(hash->size, transaction_id);
	uint32_t i;
	for (i = 0 ; i < hash->index[bucket_num]->current_subBuckets ; i++) { /* for i in subBuckets */
		if (hash->index[bucket_num]->key_buckets[i].transaction_id == transaction_id) {
			*found = True;
			return hash->index[bucket_num]->key_buckets[i].rec_offset;
		}
	}
	*found = False;
	return 0;
} 

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
