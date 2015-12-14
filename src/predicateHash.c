#include "predicateHash.h"

predicateHash* predicateCreateHash(void) {
	predicateHash *hash = malloc(sizeof(predicateHash));
	ALLOCATION_ERROR(hash);
	hash->global_depth = PREDICATE_GLOBAL_DEPTH_INIT;
	hash->size = 1 << PREDICATE_GLOBAL_DEPTH_INIT;
	hash->index = malloc(hash->size * sizeof(tidBucket *));
	ALLOCATION_ERROR(hash->index);
	uint64_t i;
	for (i = 0 ; i < hash->size ; i++)
		hash->index[i] = predicateCreateNewBucket(PREDICATE_GLOBAL_DEPTH_INIT);
	return hash;
}

Boolean_t predicateRecordsEqual(predicateSubBucket* record1, predicateSubBucket* record2) {
	if ( record1->range_start == record2->range_start &&
		 record1->range_end == record2->range_end &&
		 record1->condition->column == record2->condition->column &&
		 record1->condition->op == record2->condition->op &&
		 record1->condition->value == record2->condition->value
		)
		return True;
	return False;
}

int predicateInsertHashRecord(predicateHash* hash, predicateSubBucket* predicate_record) {
	uint64_t bucket_num = predicateHashFunction(hash->size, predicate_record);
	predicateBucket *bucket = hash->index[bucket_num];
	uint32_t i;
	for (i = 0 ; i < bucket->current_subBuckets; i++) {			/* for all subbuckets */
		if (predicateRecordsEqual(&bucket->key_buckets[i], predicate_record)) {	/* if there is a predicateSubBucket with equal fields */
			return OK_SUCCESS;
		}
	}
	/* If that was the first appearence of this key */
	if (bucket->current_subBuckets < PREDICATE_B) {	/* If there is space to insert it on that bucket (there is a free subbucket) */
		bucket->key_buckets[bucket->current_subBuckets].range_start = predicate_record->range_start;
		bucket->key_buckets[bucket->current_subBuckets].range_end = predicate_record->range_end;
		bucket->key_buckets[bucket->current_subBuckets].condition->column = predicate_record->condition->column; 
		bucket->key_buckets[bucket->current_subBuckets].condition->op = predicate_record->condition->op;
		bucket->key_buckets[bucket->current_subBuckets].condition->value = predicate_record->condition->value;
		bucket->key_buckets[bucket->current_subBuckets].open_requests = predicate_record->open_requests;
		if(bucket->key_buckets[bucket->current_subBuckets].bit_set == NULL)
			bucket->key_buckets[bucket->current_subBuckets].bit_set = createBitSet(predicate_record->bit_set->bit_size);
		copyBitSet(bucket->key_buckets[bucket->current_subBuckets].bit_set, predicate_record->bit_set);
		(bucket->current_subBuckets)++;
		return OK_SUCCESS;
	} else {
		bucket->local_depth++;
		predicateBucket *new_bucket = predicateCreateNewBucket(bucket->local_depth);
		predicateBucket *tmp_bucket = predicateCreateNewBucket(bucket->local_depth);
		predicateCopyBucketTransactions(tmp_bucket, bucket);
		if (bucket->local_depth-1 >= hash->global_depth) { /* duplicate case */
			predicateDuplicateIndex(hash);		/* duplicates and increases global depth */
			predicateFixHashPointers(hash->index, new_bucket, hash->global_depth, bucket_num);
		} else if (bucket->local_depth-1 < hash->global_depth) { /* split case*/
			predicateFixSplitPointers(hash, bucket, new_bucket, bucket_num);
		}
		predicateCleanBucket(bucket);
		uint32_t i;
		uint64_t new_hash;
		for (i = 0 ; i < PREDICATE_B ; i++) {	/* for all subBuckets in tmpBucket */
			predicateSubBucket *subbuckets = tmp_bucket->key_buckets;
			new_hash = predicateHashFunction(hash->size, subbuckets);
			// fprintf(stderr, "New hash for %zu is %zu\n",key,new_hash );
			predicateBucket* destination = hash->index[new_hash];
			uint32_t current_subBuckets = destination->current_subBuckets;
			predicateCopySubbucketTransactions(&destination->key_buckets[current_subBuckets], &subbuckets[i]);
			destination->current_subBuckets++;
		}
		predicateDestroyBucket(tmp_bucket);
		predicateInsertHashRecord(hash, predicate_record);
	}
	return OK_SUCCESS;
}  

/*Does whatever it says*/
void predicateDuplicateIndex(predicateHash * hash) {
	hash->global_depth++;
	uint64_t old_size = hash->size;
	hash->size *= 2;
	hash->index = realloc(hash->index, hash->size * sizeof(predicateBucket *));
	ALLOCATION_ERROR(hash->index);
	uint64_t i;
	for (i = old_size; i < hash->size; i++)
		hash->index[i] = NULL;
}

void predicateDestroyBucket(predicateBucket *bucket) {
	free(bucket->key_buckets);
	free(bucket);
	bucket = NULL;
}

/* fix new indexes pointers after index doublicate */
void predicateFixHashPointers(predicateBucket **index, predicateBucket *new_bucket, uint32_t global_depth, uint64_t bucket_num) {
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
void predicateFixSplitPointers(predicateHash* hash, predicateBucket* old_bucket, predicateBucket* new_bucket, uint64_t bucket_num){
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


// /* copy transactions from one predicateBucket to another one*/
void predicateCopyBucketTransactions(predicateBucket* dst, predicateBucket* src) {
	uint64_t i;
	dst->local_depth = src->local_depth;
	dst->current_subBuckets = src->current_subBuckets;
	for (i = 0 ; i < src->current_subBuckets ; i++) {	/* for i in subBuckets */
		predicateCopySubbucketTransactions(&dst->key_buckets[i], &src->key_buckets[i]);
	}
}

void predicateCopySubbucketTransactions(predicateSubBucket* dst, predicateSubBucket* src){
	dst->range_start = src->range_start;
	dst->range_end = src->range_end;
	if (dst->bit_set == NULL) {
		dst->bit_set = createBitSet(src->bit_set->bit_size);
	}
	copyBitSet(dst->bit_set, src->bit_set);
	dst->open_requests = src->open_requests;
	dst->condition->column = src->condition->column; 
	dst->condition->op = src->condition->op;
	dst->condition->value = src->condition->value;
}

/* creates an empty predicateBucket*/
predicateBucket* predicateCreateNewBucket(uint32_t local_depth) {
	predicateBucket *new_bucket = malloc(sizeof(predicateBucket));
	ALLOCATION_ERROR(new_bucket);
	new_bucket->local_depth = local_depth;
	new_bucket->current_subBuckets = 0;
	new_bucket->deletion_started = 0;
	new_bucket->key_buckets = malloc(PREDICATE_B * sizeof(predicateSubBucket));
	ALLOCATION_ERROR(new_bucket->key_buckets);
	uint32_t i;
	for (i = 0 ; i < PREDICATE_B ; i++) {	/* create subBuckets */
		new_bucket->key_buckets[i].condition = malloc(sizeof(Column_t));
		ALLOCATION_ERROR(new_bucket->key_buckets[i].condition);
		new_bucket->key_buckets[i].range_start = 0;
		new_bucket->key_buckets[i].range_end = 0;
		new_bucket->key_buckets[i].condition->column = 0;
		new_bucket->key_buckets[i].condition->op = 0;
		new_bucket->key_buckets[i].condition->value = 0;
		new_bucket->key_buckets[i].bit_set = NULL;
		new_bucket->key_buckets[i].open_requests = 0;	
	}
	return new_bucket;
}

// /* The conflict bucket is empty of transaction, just hold the local_depth */
void predicateCleanBucket(predicateBucket* conflict_bucket) {
	uint32_t i;
	for (i = 0 ; i < conflict_bucket->current_subBuckets ; i++) {	/* for i in all subBuckets*/ 
		predicateCleanSubBucket(&conflict_bucket->key_buckets[i]);
	}
	conflict_bucket->current_subBuckets = 0;
}

void predicateCleanSubBucket(predicateSubBucket* pred_subBucket) {
	pred_subBucket->range_start = 0;
	pred_subBucket->range_end = 0;
	pred_subBucket->condition->column = 0;
	pred_subBucket->condition->op = 0;
	pred_subBucket->condition->value = 0;
	if (pred_subBucket->bit_set != NULL) {
		destroyBitSet(pred_subBucket->bit_set);
	}
	pred_subBucket->bit_set = NULL;
	pred_subBucket->open_requests = 0;
}

uint64_t predicateHashFunction(uint64_t size, predicateSubBucket* predicate) {
    char str[50];
    char* str1 = str;
    sprintf(str,"%" PRIu32 "%d%zu%zu%zu", predicate->condition->column,
		     predicate->condition->op, predicate->condition->value, 
		     predicate->range_start, predicate->range_end);
    uint64_t hash = 5381;
    // printf("string: %s\n",str);
    int c;
    while ((c = *str1++) != '\0')
        hash = ((hash << 5) + hash) + c; /* hash * 33 + c */

    return hash % size;
}

// /* printsBucket various info */
void predicatePrintBucket(predicateBucket* bucket){
	fprintf(stderr, "------------------------------------------------------------\n");
	fprintf(stderr, "local_depth(%"PRIu32"), current_subBuckets(%"PRIu32")\n", bucket->local_depth, bucket->current_subBuckets);
	uint64_t i;
	for (i = 0 ; i < bucket->current_subBuckets ; i++) { 
		uint64_t range_start = bucket->key_buckets[i].range_start;
		uint64_t range_end = bucket->key_buckets[i].range_end;
		Column_t* condition = bucket->key_buckets[i].condition;
		fprintf(stderr, "\tSubBucket(%zu): range_start: %zu , range_end : %zu column %"PRIu32" op %d value %zu \n",
				i, range_start, range_end, condition->column, condition->op, condition->value);
	}
	fprintf(stderr, "------------------------------------------------------------\n");
}

// /* Prints index, the bucket pointing to and the content of the bucket */
void predicatePrintHash(predicateHash* hash){
	uint64_t i;
	for (i = 0 ; i < hash->size ; i++) { /*for every index*/
		if (hash->index[i] != NULL) { /*points somewhere*/
			fprintf(stderr, "*****Index %zu points to predicateBucket address %p*******\n", i, hash->index[i]);
			if (!hash->index[i]->current_subBuckets) {
				predicatePrintBucket(hash->index[i]);
			} else {
				predicatePrintBucket(hash->index[i]);
			}
			fprintf(stderr,"***********************************************************\n\n");
		} else {
			fprintf(stderr,"~~~~~Index (%zu) points to no bucket~~~~~\n", i);
		}
	}
}

predicateSubBucket* createPredicateSubBucket(uint64_t from, uint64_t to, uint32_t column, Op_t op, uint64_t value){
	predicateSubBucket* subBukcet = malloc(sizeof(predicateSubBucket));
	ALLOCATION_ERROR(subBukcet);
	subBukcet->range_start = from;	
	subBukcet->range_end = to;
	subBukcet->condition = malloc(sizeof(Column_t));
	ALLOCATION_ERROR(subBukcet->condition);
	subBukcet->condition->column = column;
	subBukcet->condition->op = op;
	subBukcet->condition->value = value;
	subBukcet->bit_set = NULL;
	subBukcet->open_requests = 0;
	return subBukcet;
}


BitSet_t* predicateGetBitSet(predicateHash* hash, predicateSubBucket* predicate, Boolean_t *found) {
	uint32_t i;
	uint64_t bucket_num = predicateHashFunction(hash->size, predicate);
	for (i = 0 ; i < hash->index[bucket_num]->current_subBuckets ; i++) { /* for i in subBuckets */
		if (predicateRecordsEqual(&(hash->index[bucket_num]->key_buckets[i]), predicate)) {
			*found = True;
			return hash->index[bucket_num]->key_buckets[i].bit_set;
		}
	}
	*found = False;
	return NULL;
} 

int predicateDestroyHash(predicateHash* hash) {
	uint64_t i;
	for (i = 0 ; i < hash->size ; i++) { /*for every bucket on the hash*/
		predicateBucket * bucketPtr = hash->index[i];
		if (!bucketPtr->deletion_started) { /*it is the first bucket*/
			bucketPtr->deletion_started = 1;
			bucketPtr->pointers_num = 1 << (hash->global_depth - bucketPtr->local_depth);
		}

		if (bucketPtr->pointers_num == 1 ) { /*if it is the last remaining pointer that points to the bucket*/
			free(bucketPtr->key_buckets->condition);
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

// uint8_t tryCollapseIndex(tidHash* hash) {
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
