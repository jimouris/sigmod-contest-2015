#include "murmurhash.h"
#include "predicateHash.h"

ZombieList* zombieList_create(void) {
	ZombieList* zombie_list = malloc(sizeof(ZombieList));
	ALLOCATION_ERROR(zombie_list);
	zombie_list->first_zombie = NULL;
	zombie_list->last_zombie = NULL;
	zombie_list->num_of_zombies = 0;
	return zombie_list;
}

Zombie_node * zombieList_insert_end(ZombieList* zombie_list,predicateSubBucket* subBucket) {
	Zombie_node *new_zombie = malloc(sizeof(Zombie_node));
	ALLOCATION_ERROR(new_zombie);
	new_zombie->subBucket = subBucket;
	new_zombie->next = NULL;	
	if (zombie_list->num_of_zombies == 0){
		zombie_list->first_zombie = new_zombie;
		zombie_list->last_zombie = new_zombie;
		new_zombie->prev = NULL;		
	} else {
		new_zombie->prev = zombie_list->last_zombie;
		zombie_list->last_zombie->next = new_zombie;
		zombie_list->last_zombie = new_zombie;
	}
	zombie_list->num_of_zombies++;
	return new_zombie;
}

void zombieList_remove(ZombieList *zombie_list, Zombie_node * zombie) {
	if (zombie->next != NULL) { /*there is next zombie*/
		if (zombie->prev != NULL) { /*somewhere in the middle of the list*/
			zombie->next->prev = zombie->prev;
			zombie->prev->next = zombie->next;
		} else { /*you are the first node*/
			zombie->next->prev = NULL;
			zombie_list->first_zombie = zombie->next;
		}
	} else {
		if (zombie->prev == NULL) { /*only one zombie on the zombie list*/
			zombie_list->first_zombie = NULL;
			zombie_list->last_zombie = NULL;
		} else { /*you are the last node*/
			zombie->prev->next = NULL;
			zombie_list->last_zombie = zombie->prev;
		}
	}
	zombie_list->num_of_zombies--;
	free(zombie);
}


void zombieList_destroy(ZombieList *zombie_list){
	while(zombie_list->num_of_zombies > 0)
		zombieList_remove(zombie_list, zombie_list->first_zombie);
	free(zombie_list);
}

predicateHash* predicateCreateHash(void) {
	predicateHash *hash = malloc(sizeof(predicateHash));
	ALLOCATION_ERROR(hash);
	hash->global_depth = PREDICATE_GLOBAL_DEPTH_INIT;
	hash->size = 1 << PREDICATE_GLOBAL_DEPTH_INIT;
	hash->index = malloc(hash->size * sizeof(predicateBucket *));
	ALLOCATION_ERROR(hash->index);
	uint64_t i;
	for (i = 0 ; i < hash->size ; i++)
		hash->index[i] = predicateCreateNewBucket(PREDICATE_GLOBAL_DEPTH_INIT);
	hash->zombie_list = zombieList_create();
	return hash;
}

bool predicateRecordsEqual(predicateSubBucket* record1, predicateSubBucket* record2) {
	if (record1->range_start == record2->range_start && record1->range_end == record2->range_end && record1->condition->column == record2->condition->column
			&& record1->condition->op == record2->condition->op && record1->condition->value == record2->condition->value)
	{	
		return true;
	}
	return false;
}

bool predicateRecordsEqualArguements(predicateSubBucket* record1, uint64_t from, uint64_t to, uint32_t column, Op_t op, uint64_t value) {
	if (record1->range_start == from && record1->range_end == to && record1->condition->column == column
			&& record1->condition->op == op && record1->condition->value == value)
	{	
		return true;
	}
	return false;
}

int predicateInsertBitSet(predicateHash* hash, uint64_t from, uint64_t to, uint32_t column, Op_t op, uint64_t value, BitSet_t* bit_set) {
	uint64_t bucket_num = predicateHashFunction(hash->size, from, to, column, op, value);
	predicateBucket *bucket = hash->index[bucket_num];
	uint32_t i;
	for (i = 0 ; i < bucket->current_subBuckets; i++) {			/* for all subbuckets */
		if (predicateRecordsEqualArguements(bucket->key_buckets[i], from, to, column, op, value)) {	/* if there is a predicateSubBucket with equal fields */
			bucket->key_buckets[i]->bit_set = bit_set;
			return OK_SUCCESS;
		}
	}
	return NOK_FAILURE;
}

int predicateInsertHashRecord(predicateHash* hash, uint64_t from, uint64_t to, uint32_t column, Op_t op, uint64_t value, uint64_t validationId) {
	uint64_t bucket_num = predicateHashFunction(hash->size, from, to, column, op, value);
	predicateBucket *bucket = hash->index[bucket_num];
	uint32_t i;
	for (i = 0 ; i < bucket->current_subBuckets; i++) {			/* for all subbuckets */
		if (predicateRecordsEqualArguements(bucket->key_buckets[i], from, to, column, op, value)) {	/* if there is a predicateSubBucket with equal fields */
			if(validationId != bucket->key_buckets[i]->last_validationId){
				if(bucket->key_buckets[i]->open_requests == 0){
					zombieList_remove(hash->zombie_list, bucket->key_buckets[i]->zombie);
				}
				bucket->key_buckets[i]->open_requests++;
				bucket->key_buckets[i]->last_validationId = validationId;
			}
			return OK_SUCCESS;
		}
	}
	/* If that was the first appearence of this key */
	if (bucket->current_subBuckets < PREDICATE_B) {	/* If there is space to insert it on that bucket (there is a free subbucket) */
		// bucket->key_buckets[bucket->current_subBuckets] = predicateCreateNewSubBucket(predicate_record);
		bucket->key_buckets[bucket->current_subBuckets] = createPredicateSubBucket(from, to, column, op, value, validationId);
		(bucket->current_subBuckets)++;
		return OK_SUCCESS;
	} else {
		bucket->local_depth++;
		predicateBucket *new_bucket = predicateCreateNewBucket(bucket->local_depth);
		predicateBucket *tmp_bucket = predicateCreateNewBucket(bucket->local_depth);
		predicateCopyBucketPtrs(tmp_bucket, bucket);
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
			new_hash =predicateHashFunction(hash->size, tmp_bucket->key_buckets[i]->range_start, tmp_bucket->key_buckets[i]->range_end, tmp_bucket->key_buckets[i]->condition->column, tmp_bucket->key_buckets[i]->condition->op, tmp_bucket->key_buckets[i]->condition->value);
			predicateBucket* dest_bucket = hash->index[new_hash];
			uint32_t current_subBuckets = dest_bucket->current_subBuckets;
			dest_bucket->key_buckets[current_subBuckets] = tmp_bucket->key_buckets[i];
			dest_bucket->current_subBuckets++;
		}
		free(tmp_bucket->key_buckets);
		free(tmp_bucket); /* not destroy tmpbucket, we want the pointers */
		predicateInsertHashRecord(hash, from, to, column, op, value, validationId);
	}
	return OK_SUCCESS;
}  

/* Does whatever it says */
void predicateDuplicateIndex(predicateHash * hash) {
	hash->global_depth++;
	uint64_t old_size = hash->size;
	hash->size *= 2;
	hash->index = realloc(hash->index, hash->size * sizeof(predicateBucket *));
	ALLOCATION_ERROR(hash->index);
	uint64_t i;
	for (i = old_size; i < hash->size; i++) {
		hash->index[i] = NULL;
	}
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

/* fix new indexes pointers after index splits */
void predicateFixSplitPointers(predicateHash* hash, predicateBucket* old_bucket, predicateBucket* new_bucket, uint64_t bucket_num){
	uint64_t prev_local_depth = old_bucket->local_depth-1;
	uint64_t i = 0, ld_size = 1 << prev_local_depth;
	uint32_t first_pointer = bucket_num % ld_size;
	for (i = first_pointer ; i < hash->size ; i+=(2*ld_size)) { /* i in all old_bucket pointers */
		hash->index[i] = old_bucket;
		hash->index[i+ld_size] = new_bucket;
	}
}

void predicateFixDeletePointers(predicateHash* hash, predicateBucket* bucket, predicateBucket* buddyBucket, uint64_t buddy_index) {
	uint64_t i;
	uint64_t ld_size = 1 << buddyBucket->local_depth;
	uint32_t first_pointer = buddy_index % ld_size;
	for (i = first_pointer ; i < hash->size ; i += ld_size ) {
		hash->index[i] = bucket;
	}
}

/* copy transactions from one predicateBucket to another one*/
void predicateCopyBucketPtrs(predicateBucket* dst, predicateBucket* src) {
	uint64_t i;
	dst->local_depth = src->local_depth;
	for (i = 0 ; i < src->current_subBuckets ; i++)	/* for i in subBuckets */
		dst->key_buckets[i] = src->key_buckets[i];
	dst->current_subBuckets = src->current_subBuckets;
}

predicateSubBucket* createPredicateSubBucket(uint64_t from, uint64_t to, uint32_t column, Op_t op, uint64_t value, uint64_t validationId){
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
	subBukcet->open_requests = 1;
	subBukcet->zombie = NULL;
	subBukcet->last_validationId = validationId;
	return subBukcet;
}

/* creates an empty predicateBucket*/
predicateBucket* predicateCreateNewBucket(uint32_t local_depth) {
	predicateBucket *new_bucket = malloc(sizeof(predicateBucket));
	ALLOCATION_ERROR(new_bucket);
	new_bucket->local_depth = local_depth;
	new_bucket->current_subBuckets = 0;
	new_bucket->deletion_started = 0;
	new_bucket->key_buckets = malloc(PREDICATE_B * sizeof(predicateSubBucket *));
	ALLOCATION_ERROR(new_bucket->key_buckets);
	uint32_t i;
	for (i = 0 ; i < PREDICATE_B ; i++) {	/* create subBuckets */
		new_bucket->key_buckets[i] = NULL;
	}
	return new_bucket;
}

/* The conflict bucket is empty of transaction, just hold the local_depth */
void predicateCleanBucket(predicateBucket* bucket) {
	uint32_t i;
	for (i = 0 ; i < bucket->current_subBuckets ; i++) {	/* for i in all subBuckets*/ 
		bucket->key_buckets[i] = NULL;
	}
	bucket->current_subBuckets = 0;
}

uint64_t predicateHashFunction(uint64_t size, uint64_t from, uint64_t to, uint32_t column, Op_t op, uint64_t value) {
   uint64_t hash = column;
   hash *= 37;
   hash += op;
   hash *= 37;
   hash += value;
   hash *= 37;
   hash += from;
   hash *= 37;
   hash += to;
   return hash % size;
/*murmurhash*/
	// char* str = malloc(50*sizeof(char));
	// sprintf(str,"%" PRIu32 "%d%zu%zu%zu", column, op, value, from, to);
	// uint64_t hash =  murmurhash(str,strlen(str),0) % size;
	// free(str);
	// return hash;
}

/* printsBucket various info */
void predicatePrintBucket(predicateBucket* bucket){
	fprintf(stderr, "------------------------------------------------------------\n");
	fprintf(stderr, "local_depth(%"PRIu32"), current_subBuckets(%"PRIu32")\n", bucket->local_depth, bucket->current_subBuckets);
	uint64_t i;
	for (i = 0 ; i < bucket->current_subBuckets ; i++) { 
		uint64_t range_start = bucket->key_buckets[i]->range_start;
		uint64_t range_end = bucket->key_buckets[i]->range_end;
		Column_t* condition = bucket->key_buckets[i]->condition;
		fprintf(stderr, "\tSubBucket(%zu): range_start: %zu , range_end : %zu column %"PRIu32" op %d value %zu \n",
				i, range_start, range_end, condition->column, condition->op, condition->value);
	}
	fprintf(stderr, "------------------------------------------------------------\n");
}

/* Prints index, the bucket pointing to and the content of the bucket */
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

BitSet_t* predicateGetBitSet(predicateHash* hash, uint64_t from, uint64_t to, uint32_t column, Op_t op, uint64_t value) {
	uint32_t i;
	uint64_t bucket_num = predicateHashFunction(hash->size, from, to, column, op, value);
	for (i = 0 ; i < hash->index[bucket_num]->current_subBuckets ; i++) { /* for i in subBuckets */
		predicateSubBucket* subBucket = hash->index[bucket_num]->key_buckets[i];
		if (subBucket->range_start == from && subBucket->range_end == to && subBucket->condition->column == column
			&& subBucket->condition->op == op && subBucket->condition->value == value) {

			BitSet_t* bit_set = subBucket->bit_set;
			subBucket->open_requests--;
			if(subBucket->open_requests == 0){
				//Insert into ZombieList
				subBucket->zombie = zombieList_insert_end(hash->zombie_list,subBucket);
			}
			return bit_set;
		}
	}
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
		// uint32_t j;
		if (bucketPtr->pointers_num == 1 ) { /*if it is the last remaining pointer that points to the bucket*/
			predicateDestroyBucket(bucketPtr);
		}else{
			bucketPtr->pointers_num--;
		}
		hash->index[i] = NULL;
		bucketPtr = NULL;
	}
	zombieList_destroy(hash->zombie_list);
	free(hash->index);
	free(hash);
	hash = NULL;
	return OK_SUCCESS;
}

int predicateDeleteHashRecord(predicateHash* hash, predicateSubBucket* predicate_record) {
	uint64_t bucket_num = predicateHashFunction(hash->size, predicate_record->range_start, predicate_record->range_end, predicate_record->condition->column, predicate_record->condition->op, predicate_record->condition->value);
	int deletion_found = predicateForgetSubBucket(hash,bucket_num,predicate_record);
	if (!deletion_found)
		return 0;
	return 1;
}

void predicateMoveSubBucketsLeft(predicateBucket* bucket, uint32_t index) {
	uint32_t i;
	for(i = index ; i < bucket->current_subBuckets; i++) {
			bucket->key_buckets[i-1] = bucket->key_buckets[i];
			bucket->key_buckets[i] = NULL;
	}
}

int predicateForgetSubBucket(predicateHash* hash, uint64_t bucket_num, predicateSubBucket* predicate_record) {
	predicateBucket *bucket = hash->index[bucket_num];
	uint32_t i;
	for (i = 0 ; i < bucket->current_subBuckets; i++) {	/* for all subbuckets */
		if ( predicateRecordsEqual(bucket->key_buckets[i],predicate_record) ) {	/* predicateSubBucket for deletion found*/
			//physical remove of the SubBukcet that contains the Key
			predicateDestroySubBucket(bucket->key_buckets[i]);
			//cleanSubBucket(&bucket->key_buckets[i]);
			predicateMoveSubBucketsLeft(bucket,i+1);
			bucket->current_subBuckets--;
			/*now try to merge Buckets*/
			predicateTryMergeBuckets(hash,bucket_num);
			return 1;
		}
	}
	return 0;
}

uint8_t predicateTryCollapseIndex(predicateHash* hash) {
	uint8_t canCollapse = 0;
	if (hash->size > 1) {
		canCollapse = 1;
		uint64_t i,j;
		for (i = 0,j=hash->size/2; i < hash->size/2; i++,j++) {
			if (hash->index[i] != hash->index[j]) { /*we can not doublicate*/
				canCollapse = 0;
				break;
			}
		}
	}
	if (canCollapse) {
		hash->global_depth --;
		hash->size /= 2;
		hash->index = realloc(hash->index,hash->size * sizeof(predicateBucket *));
		ALLOCATION_ERROR(hash->index);
	}
	return canCollapse;
}

void predicateTryMergeBuckets(predicateHash* hash, uint64_t bucket_num ) {
	if (hash->global_depth == 0) { /*base case for recursion*/
		return;
	}
	predicateBucket *bucket = hash->index[bucket_num];
	uint32_t ld_oldSize = 1 << (bucket->local_depth - 1);
	uint64_t buddy_index;
	/*find its buddy bucket and store its index to buddy_index*/
	if (bucket_num + ld_oldSize < hash->size) { /*buddy bucket is under*/
		buddy_index = bucket_num + ld_oldSize;
		// fprintf(stderr,"It is under with buddy_index %llu\n",buddy_index);
	} else { /*buddy bucket is above*/
		buddy_index = bucket_num - ld_oldSize;
		// fprintf(stderr,"It is above with buddy_index %llu\n",buddy_index);
	}
	if (bucket != hash->index[buddy_index] && bucket->local_depth == hash->index[buddy_index]->local_depth) { /*we have a buddy and not the the bucket itself*/
		predicateBucket *buddyBucket = hash->index[buddy_index];
		uint64_t mergedBucket_entries = bucket->current_subBuckets + buddyBucket->current_subBuckets;
		if (mergedBucket_entries <= PREDICATE_B) { /*we can merge the two subBuckets*/
			// fprintf(stderr,"predicateBucket(%zu) - Buddy(%zu) mergedBucket_entries (%zu) \n",bucket_num,buddy_index,mergedBucket_entries);
			uint64_t i,j;
			for (i = bucket->current_subBuckets,j=0; i < mergedBucket_entries ; i++,j++) {
				bucket->key_buckets[i] = buddyBucket->key_buckets[j];
			}
			predicateFixDeletePointers(hash, bucket, buddyBucket, buddy_index);
			bucket->current_subBuckets = mergedBucket_entries;
			bucket->local_depth--;
			predicateDestroyBucketNoSubBuckets(buddyBucket);
			if (predicateTryCollapseIndex(hash)) {
				predicateTryMergeBuckets(hash, bucket_num % (1 << bucket->local_depth));
			}
		}
	}
}

void predicateDestroyBucket(predicateBucket *bucket) {
	uint32_t i=0;
	for (i = 0 ; i < bucket->current_subBuckets ; i++) {
		predicateDestroySubBucket(bucket->key_buckets[i]);
	}
	free(bucket->key_buckets);
	free(bucket);
	bucket = NULL;
}

/*use it on forget only to keep SubBuckets as they will be pointed by other bucket*/
void predicateDestroyBucketNoSubBuckets(predicateBucket *bucket) {
	free(bucket->key_buckets);
	free(bucket);
}

void predicateDestroySubBucket(predicateSubBucket *sub_bucket) {
	free(sub_bucket->condition);
	if(sub_bucket->bit_set != NULL)
		destroyBitSet(sub_bucket->bit_set);
	free(sub_bucket);
	sub_bucket = NULL;
}

void forgetPredicateIndex(predicateHash* hash, uint64_t transaction_id) {
	Zombie_node* node = hash->zombie_list->first_zombie;
	while(node != NULL){
		Zombie_node* to_delete = NULL;
		if(node->subBucket->range_start < transaction_id){
			to_delete = node;
			predicateDeleteHashRecord(hash,node->subBucket);
		}
		node = node->next;
		if(to_delete != NULL)
			zombieList_remove(hash->zombie_list, to_delete);
	}
}