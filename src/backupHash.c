
// /*insert Record to Hash*/
int insertHashRecord(Hash* hash, Key key, RangeArray* rangeArray) {
	uint64_t bucket_num = hashFunction(hash->size, key);
	Bucket *bucket = hash->index[bucket_num];
	uint32_t i;
	for (i = 0 ; i < bucket->current_subBuckets; i++) {			/* for all subbuckets */
		if (bucket->key_buckets[i].key == key) {				/* if there is a subBucket with this key */
			uint64_t current_entries = bucket->key_buckets[i].current_entries;
			if (current_entries == bucket->key_buckets[i].limit) { 	/* if there isn't enough free space, realloc */
				bucket->key_buckets[i].transaction_range = realloc(bucket->key_buckets[i].transaction_range, ((current_entries)+C) * sizeof(RangeArray));
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
			splitBucket(hash, bucket_num, rangeArray, 1, key);
		} else if (bucket->local_depth < hash->global_depth) { // split bucket
			splitBucket(hash, bucket_num, rangeArray, 0, key);
		}

	}
	return OK_SUCCESS;
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
	destroyBucket(tmp_bucket,B+1);
	// if all entries gone to one bucket 
	if (flag1 == 0) {
		destroyBucket(hash->index[bucket_num],B);
		hash->index[bucket_num] = hash->index[new_bucket_hash];
		splitBucket(hash, new_bucket_hash, rangeArray, 1, key);
	} else if (flag2 == 0) {
		destroyBucket(new_bucket,B);
		uint64_t old_size;
		if (doublicate_index_flag) {
			old_size = 1 << (hash->global_depth-1);
		}else{
			old_size = 1 << hash->global_depth;
		}

		if ( hash->index[old_size+bucket_num] == NULL) {
			hash->index[old_size+bucket_num] = hash->index[bucket_num];
		}
		splitBucket(hash, bucket_num, rangeArray, 1, key);
	}
}