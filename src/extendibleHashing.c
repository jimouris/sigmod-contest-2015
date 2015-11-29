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
		hash->index[i] = createNewBucket(GLOBAL_DEPTH_INIT);
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
	} else {
		bucket->local_depth++;
		Bucket *new_bucket = createNewBucket(bucket->local_depth);
		Bucket *tmp_bucket = createNewBucket(bucket->local_depth);
		copyBucketTransactions(tmp_bucket, bucket);
		if (bucket->local_depth-1 >= hash->global_depth) { /* duplicate case */
			duplicateIndex(hash);	/* duplicates and increases global depth */
			fixHashPointers(hash->index, new_bucket, hash->global_depth, bucket_num);
		} else if (bucket->local_depth-1 < hash->global_depth) { /* split case*/
			fixSplitPointers(hash, bucket, new_bucket, bucket_num);
		}
		cleanBucket(bucket);
		uint32_t i;
		uint64_t new_hash;
		for (i = 0 ; i < B ; i++) {	/* for all subBuckets in tmpBucket */
			SubBucket *subbuckets = tmp_bucket->key_buckets;
			Key sub_key = subbuckets[i].key;
			new_hash = hashFunction(hash->size, sub_key);
			// fprintf(stderr, "New hash for %zu is %zu\n",key,new_hash );
			Bucket* destination = hash->index[new_hash];
			uint32_t current_subBuckets = destination->current_subBuckets;
			copySubbucketTransactions(&destination->key_buckets[current_subBuckets], &subbuckets[i]);
			destination->current_subBuckets++;
		}
		destroyBucket(tmp_bucket);
		insertHashRecord(hash, key, rangeArray);
	}
	return OK_SUCCESS;
}  

/*Does whatever it says*/
void duplicateIndex(Hash * hash) {
	hash->global_depth++;
	uint64_t old_size = hash->size;
	hash->size *= 2;
	hash->index = realloc(hash->index, hash->size * sizeof(Bucket *));
	ALLOCATION_ERROR(hash->index);
	uint64_t i;
	for (i = old_size; i < hash->size; i++)
		hash->index[i] = NULL;
}

void destroyBucket(Bucket *bucket) {
	uint32_t i;
	for (i = 0 ; i < B ; i++) { /*for each subBucket*/
		free(bucket->key_buckets[i].transaction_range);
	}
	free(bucket->key_buckets);
	free(bucket);
	bucket = NULL;
}

/* fix new indexes pointers after index doublicate */
void fixHashPointers(Bucket **index, Bucket *new_bucket, uint32_t global_depth, uint64_t bucket_num) {
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
void fixSplitPointers(Hash* hash, Bucket* old_bucket, Bucket* new_bucket, uint64_t bucket_num){
	uint64_t prev_local_depth = old_bucket->local_depth-1;
	uint64_t i;
	uint64_t ld_size = 1 << prev_local_depth;
	uint32_t first_pointer = bucket_num % ld_size;
	for (i = first_pointer ; i < hash->size ; i+=(2*ld_size)) { /* i in all old_bucket pointers */
		hash->index[i] = old_bucket;
		hash->index[i+ld_size] = new_bucket;
	}
}

void fixDeletePointers(Hash* hash, Bucket* bucket, Bucket* buddyBucket, uint64_t buddy_index) {
	uint64_t i;
	uint64_t ld_size = 1 << buddyBucket->local_depth;
	uint32_t first_pointer = buddy_index % ld_size;
	for (i = first_pointer ; i < hash->size ; i += ld_size ) {
		hash->index[i] = bucket;
	}
}


/* copy transactions from one Bucket to another one*/
void copyBucketTransactions(Bucket* dst, Bucket* src) {
	uint64_t i;
	dst->local_depth = src->local_depth;
	dst->current_subBuckets = src->current_subBuckets;

	for (i = 0 ; i < src->current_subBuckets ; i++) {	/* for i in subBuckets */
		copySubbucketTransactions(&dst->key_buckets[i], &src->key_buckets[i]);
	}
}

void copySubbucketTransactions(SubBucket* dst, SubBucket* src){
	uint64_t j;
	dst->key = src->key;
	dst->current_entries = src->current_entries;

	if (dst->current_entries > dst->limit) {
		dst->transaction_range = realloc(dst->transaction_range, src->limit*sizeof(RangeArray));
		ALLOCATION_ERROR(dst->transaction_range);
	}
	dst->limit = src->limit;
	for (j = 0 ; j < src->current_entries ; j++) { /* for j in transactionRange */
		dst->transaction_range[j].transaction_id = src->transaction_range[j].transaction_id;			
		dst->transaction_range[j].rec_offset = src->transaction_range[j].rec_offset;
	}
}

/* creates an empty Bucket*/
Bucket* createNewBucket(uint32_t local_depth) {
	Bucket *new_bucket = malloc(sizeof(Bucket));
	ALLOCATION_ERROR(new_bucket);
	new_bucket->local_depth = local_depth;
	new_bucket->current_subBuckets = 0;
	new_bucket->deletion_started = 0;
	new_bucket->key_buckets = malloc(B * sizeof(SubBucket));
	ALLOCATION_ERROR(new_bucket->key_buckets);
	uint32_t i, j;
	for (i = 0 ; i < B ; i++) {	/* create subBuckets */
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
	for (i = 0 ; i < conflict_bucket->current_subBuckets ; i++) {	/* for i in all subBuckets */
		cleanSubBucket(&conflict_bucket->key_buckets[i]);
	}
	conflict_bucket->current_subBuckets = 0;
}

void cleanSubBucket(SubBucket* subBucket) {
	uint64_t j,iter = subBucket->current_entries; /* reduce iterations */
	subBucket->key = 0;
	if (subBucket->current_entries > C ) { /* we must realloc to the default capacity C */
		subBucket->transaction_range = realloc(subBucket->transaction_range,C * sizeof(RangeArray));
		ALLOCATION_ERROR(subBucket->transaction_range);
		subBucket->limit = C;
		iter = C;
	}
	for (j = 0 ; j < iter ; j++) {	/* for j in transaction range */
		subBucket->transaction_range[j].transaction_id = 0;
		subBucket->transaction_range[j].rec_offset = 0;
	}
	subBucket->current_entries = 0;
}

uint64_t hashFunction(uint64_t size, uint64_t x) {
    return (x % size);
    // x = ((x >> 16) ^ x) * 0x45d9f3b;
	//    x = ((x >> 16) ^ x) * 0x45d9f3b;
	//    x = ((x >> 16) ^ x);
	// return ((x*2654435761+1223) % size);
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
	*current_entries = 0;
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
		if (!bucketPtr->deletion_started) { /*it is the first bucket*/
			bucketPtr->deletion_started = 1;
			bucketPtr->pointers_num = 1 << (hash->global_depth - bucketPtr->local_depth);
		}

		if (bucketPtr->pointers_num == 1 ) { /*if it is the last remaining pointer that points to the bucket*/
			for (j = 0 ; j < B ; j++) {
				free(bucketPtr->key_buckets[j].transaction_range);
			}
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
int deleteHashRecord(Hash* hash, Key key) {
	uint64_t bucket_num = hashFunction(hash->size, key);
	// Bucket *bucket = hash->index[bucket_num];
	int deletion_found = deleteSubBucket(hash,bucket_num,key);
	if (!deletion_found)
		return 0;
	return 1;
}

void moveSubBucketsLeft(Bucket* bucket, uint32_t index) {
		uint32_t i;
		for(i = index ; i < bucket->current_subBuckets; i++) {
			copySubbucketTransactions(&bucket->key_buckets[i-1],&bucket->key_buckets[i]);
			cleanSubBucket(&bucket->key_buckets[i]);
	}
}

int deleteSubBucket(Hash* hash, uint64_t bucket_num, Key key) {
	Bucket *bucket = hash->index[bucket_num];
	uint32_t i;
	for (i = 0 ; i < bucket->current_subBuckets; i++) {	/* for all subbuckets */
		if (bucket->key_buckets[i].key == key) {	/* SubBucket for deletion found*/
			/* physical remove of the SubBukcet that contains the Key */
			cleanSubBucket(&bucket->key_buckets[i]);
			moveSubBucketsLeft(bucket,i+1);
			bucket->current_subBuckets--;
			/*now try to merge Buckets*/
			tryMergeBuckets(hash,bucket_num);
			return 1;
		}
	}
	return 0;
}

unsigned char tryCollapseIndex(Hash* hash) {
	unsigned char canCollapse = 0;
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
		hash->index = realloc(hash->index,hash->size * sizeof(Bucket *));
		ALLOCATION_ERROR(hash->index);
	}
	return canCollapse;
}

void tryMergeBuckets(Hash* hash, uint64_t bucket_num ) {
	Bucket *bucket = hash->index[bucket_num];
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
		Bucket *buddyBucket = hash->index[buddy_index];
		uint64_t mergedBucket_entries = bucket->current_subBuckets + buddyBucket->current_subBuckets;
		if (mergedBucket_entries <= B) { /*we can merge the two subBuckets*/
			fprintf(stderr,"Bucket(%zu) - Buddy(%zu)\n",bucket_num,buddy_index);
			uint64_t i,j;
			for (i = bucket->current_subBuckets,j=0; i < mergedBucket_entries ; i++,j++) {
				copySubbucketTransactions(&bucket->key_buckets[i],&buddyBucket->key_buckets[j]);
			}
			fixDeletePointers(hash, bucket, buddyBucket, buddy_index);
			bucket->current_subBuckets = mergedBucket_entries;
			bucket->local_depth--;
			destroyBucket(buddyBucket);
			if (tryCollapseIndex(hash)) {
				tryMergeBuckets(hash, bucket_num % (1 << bucket->local_depth));
			}
		}
	}

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
