#include "extendibleHashing.h"

Hash* createHash() {
	Hash *hash = malloc(sizeof(Hash));
	ALLOCATION_ERROR(hash);
	hash->global_depth = GLOBAL_DEPTH_INIT;
	hash->size = 1 << GLOBAL_DEPTH_INIT;
	hash->index = malloc(hash->size * sizeof(Bucket *));
	ALLOCATION_ERROR(hash->index);
	uint64_t i, j;
	for (i = 0 ; i < hash->size ; i++) {
		hash->index[i] = malloc(sizeof(Bucket));
		ALLOCATION_ERROR(hash->index[i]);
		hash->index[i]->local_depth = GLOBAL_DEPTH_INIT;
		hash->index[i]->capacity = C;
		hash->index[i]->current_entries = 0 ;
		hash->index[i]->transaction_range = malloc(hash->index[i]->capacity * sizeof(t_t));
		ALLOCATION_ERROR(hash->index[i]->transaction_range);
		for (j = 0 ; j < hash->index[i]->capacity ; j++)
			hash->index[i]->transaction_range[j].rec = NULL;
	}
	return hash;
}

/*copy transactions from one Bucket to another one*/
void copyBucketTransactions(Bucket* dest,Bucket* src) {
	int i = 0 ;
	for (i = 0 ; i < src->current_entries ; i++) {
		dest->transaction_range[i].transaction_id = src->transaction_range[i].transaction_id;
		dest->transaction_range[i].rec = src->transaction_range[i].rec;
	}
}

/*tempBucket hash C+1 places.C places for the conflict Bucket and 1 place for the new Key*/
Bucket * createTempBucket(){
	Bucket *tmp_bucket = malloc(sizeof(Bucket));
	ALLOCATION_ERROR(tmp_bucket);
	tmp_bucket->capacity = C; //we do not use it
	tmp_bucket->transaction_range = malloc((tmp_bucket->capacity+1) * sizeof(t_t));
	ALLOCATION_ERROR(tmp_bucket->transaction_range);
	uint64_t i = 0;
	for (i = 0 ; i < tmp_bucket->capacity ; i++ ) {
		tmp_bucket->transaction_range[i].transaction_id = 0;
		tmp_bucket->transaction_range[i].rec = NULL;
	}
	return tmp_bucket;
}

/*creates an empty Bucket(no transactions) with local_depth same of recently increased conflict_bucket*/
Bucket * createNewBucket(Bucket* conflict_bucket){
	Bucket *new_bucket = malloc(sizeof(Bucket));
	ALLOCATION_ERROR(new_bucket);
	new_bucket->capacity = C;
	new_bucket->local_depth = conflict_bucket->local_depth ;
	new_bucket->current_entries = 0;
	new_bucket->transaction_range = malloc(new_bucket->capacity * sizeof(t_t));
	ALLOCATION_ERROR(new_bucket->transaction_range);
	uint64_t i = 0;
	for (i = 0 ; i < new_bucket->capacity ; i++ ) {
		new_bucket->transaction_range[i].transaction_id = 0;
		new_bucket->transaction_range[i].rec = NULL;
	}
	return new_bucket;
}

/*printsBucket various info*/
void printBucket(Bucket * bucket){
	fprintf(stderr,"------------------------------------------------------------\n");
	fprintf(stderr,"local_depth(%zu), current_entries(%zu), capacity(%zu)\n",bucket->local_depth,bucket->current_entries,bucket->capacity);
	int j = 0 ;
	for (j = 0 ; j < bucket->current_entries ; j++) { //
		uint64_t tid = bucket->transaction_range[j].transaction_id;
		fprintf(stderr,"Record(%d) : %zu\n",j,tid);
	}
	fprintf(stderr,"------------------------------------------------------------\n");
}

/*Prints index, the bucket pointing to and the content of the bucket.*/
void printHash(Hash* hash){
	 int i;
	 for (i = 0 ; i < hash->size ; i++) { /*for every index*/
	 	if (hash->index[i] != NULL) { /*points somewhere*/
	 		fprintf(stderr, "*****Index %d points to Bucket address %p*******\n",i,hash->index[i]);
	 		fprintf(stderr,"Bucket has local_depth(%zu) capacity(%zu) and current_entries(%zu) \n",hash->index[i]->local_depth,hash->index[i]->capacity,hash->index[i]->current_entries);
	 		if (!hash->index[i]->current_entries) {
	 			fprintf(stderr,"Bucket has no entries yet\n");
	 		} else {
	 			fprintf(stderr,"Bucket has %zu records\n",hash->index[i]->current_entries);
	 			printBucket(hash->index[i]);
	 		}
	 		fprintf(stderr,"***********************************************************\n");
	 	} else {
	 		fprintf(stderr,"~~~~~Index (%d) points to no bucket~~~~~\n",i);
	 	}
	 }
}

/*The conflict bucket is empty of transaction, just hold the local_depth*/
void cleanConflictBucket(Bucket* conflict_bucket,Bucket* tmp_bucket) {
	int i = 0 ;
	for (i = 0 ; i < conflict_bucket->current_entries ; i++ ) {
		conflict_bucket->transaction_range[i].transaction_id = 0;
		conflict_bucket->transaction_range[i].rec = NULL;
	}
	conflict_bucket->current_entries = 0;
	conflict_bucket->capacity = C;
	conflict_bucket->local_depth = tmp_bucket->local_depth;
}

/*Adds the Key that caused the conflict as the last element of the temp_bucket transactions array*/
void addNewKeyToTempBucket(Bucket * temp_bucket,JournalRecord_t* rec) {
	temp_bucket->current_entries = C + 1;
	temp_bucket->transaction_range[C].transaction_id = rec->transaction_id;
	temp_bucket->transaction_range[C].rec = rec;
}

/*
 * We use it on Recursion. This function recreates the conflict_bucket (remember it was empty from
 * the cleanConflictBucket) and destroys the malloced tmp_bucket. After this function call, comes 
 * the recursion on spliBucket
*/
void destroyTempBucketRecreateConflict(Bucket * conflict_bucket,Bucket *tmp_bucket) {
	int i;
	for (i = 0 ; i < C ; i++) {
		conflict_bucket->transaction_range[i].transaction_id = tmp_bucket->transaction_range[i].transaction_id;
		conflict_bucket->transaction_range[i].rec = tmp_bucket->transaction_range[i].rec;
	}
	conflict_bucket->capacity = C;
	conflict_bucket->current_entries = C;
	conflict_bucket->local_depth = tmp_bucket->local_depth;
	free(tmp_bucket->transaction_range);
	free(tmp_bucket);
	tmp_bucket = NULL;
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
void splitBucket(Hash* myhash, uint64_t bucket_num, JournalRecord_t* rec, int doublicate_index_flag) {
	/* allocate a new tmp bucket and the new one (for the split) */
	if (doublicate_index_flag){
		doublicateIndex(myhash);	
	}
	myhash->index[bucket_num]->local_depth++;
	Bucket *new_bucket = createNewBucket(myhash->index[bucket_num]);
	Bucket *tmp_bucket = createTempBucket();
	tmp_bucket->local_depth = myhash->index[bucket_num]->local_depth; //need it for destroyTempBucketRecreateConflict
	copyBucketTransactions(tmp_bucket, myhash->index[bucket_num]);
	addNewKeyToTempBucket(tmp_bucket, rec); //change tid to key later
	cleanConflictBucket(myhash->index[bucket_num], tmp_bucket);
	/* flags to check if the split actually changed tids or we have to doublicate index again */
	int i;
	int flag1 = 0, flag2 = 0;
	uint64_t new_bucket_hash = 0;
	for (i = 0 ; i <= C ; i++) { //scan the tmp_bucket
		uint64_t tmp_tid = tmp_bucket->transaction_range[i].transaction_id;
		uint64_t new_hash = hashFunction(1 << myhash->global_depth, tmp_tid);
		if (new_hash == bucket_num && myhash->index[bucket_num]->current_entries < C) { //tid on old bucket
			uint64_t current_entries = myhash->index[bucket_num]->current_entries;
			myhash->index[bucket_num]->transaction_range[current_entries].transaction_id = tmp_tid;
			myhash->index[bucket_num]->transaction_range[current_entries].rec = tmp_bucket->transaction_range[i].rec;
			myhash->index[bucket_num]->current_entries++;
			flag1 = 1;
		} else if (new_hash != bucket_num && new_bucket->current_entries < C) { //tid on new bucket
			uint64_t current_entries = new_bucket->current_entries;
			new_bucket->transaction_range[current_entries].transaction_id = tmp_tid;
			new_bucket->transaction_range[current_entries].rec = tmp_bucket->transaction_range[i].rec;
			new_bucket->current_entries++;
			flag2 = 1;
			new_bucket_hash = new_hash;
		}
	}
	fixHashPointers(myhash->index, new_bucket, myhash->global_depth, bucket_num);
	// if all entries gone to one bucket 
	if (flag1 == 0){
		destroyTempBucketRecreateConflict(myhash->index[bucket_num],tmp_bucket);
		splitBucket(myhash, new_bucket_hash, rec,1);
	} else if (flag2 == 0){
		destroyTempBucketRecreateConflict(myhash->index[bucket_num],tmp_bucket);
		splitBucket(myhash, bucket_num, rec,1);
	} else {	
		free(tmp_bucket->transaction_range);
		free(tmp_bucket);
		tmp_bucket = NULL;
	}
}

/* fix new indexes pointers after index doublicate or just splits pointers */
void fixHashPointers(Bucket **index, Bucket *new_bucket, uint64_t global_depth, uint64_t bucket_num) {
	int i, j;
	uint64_t old_size = 1 << (global_depth-1);
	for (i = 0, j = old_size ; i < old_size ; i++, j++) {
		if (i == bucket_num)
			index[j] = new_bucket;
		else
			index[j] = index[i];
	}
}

/*insert Record to Hash*/
int insertHashRecord(Hash* hash, Key key, RangeArray* rangeArray, JournalRecord_t* rec) {
	/*important : change 'tid' with 'key' later */
	uint64_t bucket_num = hashFunction(hash->size, key); 
	Bucket *bucket = hash->index[bucket_num];
	if (bucket->current_entries < bucket->capacity) { // If there is space to insert it on the bucket
		uint64_t current_entries = bucket->current_entries; 
		bucket->transaction_range[current_entries].transaction_id = rec->transaction_id;
		bucket->transaction_range[current_entries].rec = rec;
		bucket->current_entries++;
		return 0; // OK_SUCCESS
	} else { // if there is no space
		if (bucket->local_depth == hash->global_depth) { // one pointer per bucket -> doublicate index
			splitBucket(hash, bucket_num, rec, 1);
		} else if (bucket->local_depth < hash->global_depth) { // split bucket
			splitBucket(hash, bucket_num, rec, 0);
		}
	}
	return 0;
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

int deleteHashRecordByKey(Hash* hash,Key key) { 
	uint64_t bucket_num = hashFunction(hash->size,key); //get bucket num where you would delete
	Bucket *bucket = hash->index[bucket_num];
	int i;
	for (i = 0 ; i < bucket->current_entries ; i++) {
		if (bucket->transaction_range[i].rec->column_values[0] == key) { //found the record that you want to delete
			bucket->current_entries --;
			if (bucket->current_entries == 0) { //empty bucket

			}
		}
	}
	return 1;
}

// OK_SUCCESS deleteJournalRecord(Hash*, Key, int transaction_id) {

// } 

RangeArray* getHashRecord(Hash* hash, Key key) {
	uint64_t bucket_num = hashFunction(hash->size, key);
	return hash->index[bucket_num]->transaction_range;
} 

JournalRecord_t* getHashRecord2(Hash* hash, Key key) {
	uint64_t bucket_num = hashFunction(hash->size, key);
	return searchIndexByKey(hash,bucket_num,key);
}

/* Binary Search for first appearance */
JournalRecord_t* searchIndexByKey(Hash* hash,uint64_t bucket_num,uint64_t keyToSearch) {
	Bucket *bucket = hash->index[bucket_num];
	int i;
	for (i = 0 ; i < bucket->current_entries ; i++) {
		if (bucket->transaction_range[i].rec->column_values[0] == keyToSearch) {
			return bucket->transaction_range[i].rec;
		}
	}
	return NULL;
}


// List<Record> getHashRecords(Hash*, Key, int range_start, int range_end) {

// }

int destroyHash(Hash* hash) {
	uint64_t i, j;
	fprintf(stderr, "deleting: size is %zd\n", hash->size);
	for (i = 0 ; i < hash->size ; i++) {	/* for all buckets */
		if (hash->index[i] != NULL) {	/* if is not already freed */
			if (hash->index[i]->transaction_range != NULL) {
				for (j = 0 ; j < hash->index[i]->current_entries ; j++) {
					hash->index[i]->transaction_range[j].rec = NULL;
				}
				free(hash->index[i]->transaction_range);
				hash->index[i]->transaction_range = NULL;
			}
			/*
			* oi 2 apo katw grammes exoun to provlhma, otan tis 3esxoliazw vgazei errors to valgrind
			* profanws to error den einai sto free
			*/
			// free(hash->index[i]);
			// hash->index[i] = NULL;
		}
	}
	free(hash);
} 


/* shmeiwsh: isws kapou 3exname na kanoume current entries-- 
* 8elei ligo psa3imo alla uparxei periptwsh na exw dikio 
*/