#include "parser.h"
#include "scheduler.h"

static uint32_t* schema = NULL;

static thread_arg_t* thread_array;

Flush_t* last_flush = NULL;
int flushes = 0;

extern int* modes;


Journal_t** processDefineSchema(DefineSchema_t *s, int *relation_count, int* modes) {
	uint64_t i;
	if (schema == NULL)
		free(schema);
	schema = malloc(sizeof(uint32_t) * s->relationCount);
	*relation_count = s->relationCount;
	ALLOCATION_ERROR(schema);
	Journal_t** journal_array = malloc(s->relationCount * sizeof(Journal_t*));
	ALLOCATION_ERROR(journal_array);
	for (i = 0; i < s->relationCount; i++) {
		schema[i] = s->columnCounts[i];
		journal_array[i] = createJournal(i, modes);
	}

	if(modes[2] != 0){
		thread_array = malloc(sizeof(thread_arg_t) * modes[2]);
		ALLOCATION_ERROR(thread_array);
		for(i = 0; i < modes[2]; i++){
			thread_array[i].validation_array = malloc(THREAD_VAL_ARRAY_SIZE * sizeof(ValidationQueries_t*));
			ALLOCATION_ERROR(thread_array[i].validation_array);
			thread_array[i].size = THREAD_VAL_ARRAY_SIZE;
			thread_array[i].validation_num = 0;
			thread_array[i].result_array = NULL;
			thread_array[i].first_val_id = 0;
			thread_array[i].journal_array = NULL;
		}
	}

	return journal_array;
}

void processTransaction(Transaction_t *t, Journal_t** journal_array) {
	uint64_t i,j;
	const char* reader = t->operations;
	for (i = 0; i < t->deleteCount; i++) {
		const TransactionOperationDelete_t* o = (TransactionOperationDelete_t*)reader;
		for (j = 0; j < o->rowCount; j++){
			//Find the latest insert record.
			uint64_t key = o->keys[j];
			JournalRecord_t* last_insertion = getLastRecord(journal_array[o->relationId], key);
			if (last_insertion != NULL && last_insertion->dirty_bit == false) {
				//Insert the JournalRecord.
				insertJournalRecordCopy(journal_array[o->relationId], last_insertion, t->transactionId, true); 
			} /* else { // the key doesn't exist, skip it } */
		}
		reader += sizeof(TransactionOperationDelete_t) + (sizeof(uint64_t) * o->rowCount);
	}
	for (i = 0; i < t->insertCount; i++) {
		const TransactionOperationInsert_t* o = (TransactionOperationInsert_t*)reader;
		for(j = 0; j < o->rowCount; j++){
			const uint64_t *values = o->values + j*schema[o->relationId];
			insertJournalRecord(journal_array[o->relationId], t->transactionId, schema[o->relationId], values, false);			
		}
		reader += sizeof(TransactionOperationInsert_t) + (sizeof(uint64_t) * o->rowCount * schema[o->relationId]);
	}
}


void processValidationQueries(ValidationQueries_t *v, Journal_t** journal_array, ValidationList_t* validation_list) {
	ValidationQueries_t* val_query = v;
	char* reader = v->queries;
	uint32_t i,j;
	for (i = 0; i < v->queryCount; i++) {
		Query_t* query = (Query_t*)reader;
		Journal_t* journal = journal_array[query->relationId];
		/*Bring C0==X first*/
		// uint32_t j;
		// for(j = 0; j<query->columnCount; j++){
		// 	if(query->columns[j].column == 0 && query->columns[j].op == Equal){
		// 		Column_t temp = query->columns[j];
		// 		query->columns[j] = query->columns[0];
		// 		query->columns[0] = temp;
		// 		break;
		// 	}
		// }
		/*Bring C0==X first*/


		qsort(query->columns, query->columnCount, sizeof(Column_t), cmp_col);
		if (journal->predicate_index != NULL) {
			for(j = 0; j < query->columnCount; j++) {
				Column_t* predicate = &query->columns[j];
				// predicateSubBucket* predicateSubBucket = createPredicateSubBucket(val_query->from, val_query->to, predicate->column, predicate->op, predicate->value);
				predicateInsertHashRecord(journal->predicate_index,val_query->from, val_query->to, predicate->column, predicate->op, predicate->value, val_query->validationId);
			}	
		}
		
		reader += sizeof(Query_t) + (sizeof(Column_t) * query->columnCount);
	}
	// printValidation(val_query);
	validationListInsert(validation_list, val_query);
}


int cmp_col(const void *p1, const void *p2) {
	const Column_t *f1 = (Column_t*) p1;
	const Column_t *f2 = (Column_t*) p2;
	if(f1->op == Equal){
		if(f2->op == Equal && f2->column == 0){
			return 1;
		}
		return -1;
	} else if(f2->op == Equal) {
		return 1;
	}
	return 0;
}
void* threadFunction(void* thread_arg){
	thread_arg_t* arg = (thread_arg_t*) thread_arg; 
	uint64_t i;
	for(i = 0; i < arg->validation_num; i++){
		if(checkValidation(arg->journal_array, arg->validation_array[i])){
			uint64_t position = arg->validation_array[i]->validationId - arg->first_val_id;
			arg->result_array[position] = 1;
		}
	}
	return NULL;
}

void processFlush(Flush_t *fl, Journal_t** journal_array, ValidationList_t* validation_list) {
	last_flush = fl;
	if(modes[2] != 0){
		flushes++;
		if(flushes == modes[3]){
			flushes = 0;
			static uint64_t last_validation_id = 0;
			// fprintf(stderr, "%zu\n",fl->validationId );
			uint64_t remove_count = 0;
			uint64_t num_of_validations = fl->validationId - last_validation_id + 1;	//Number of Validations to evaluate
			uint8_t* result_array = calloc(num_of_validations,sizeof(uint8_t));
			int thread_num = modes[2];
			uint64_t max_validations = num_of_validations / thread_num + 1;
			int i;
			if (max_validations > thread_array[0].size){
				for(i = 0; i < thread_num; i++){
					thread_array[i].validation_array = realloc(thread_array[i].validation_array, max_validations * sizeof(ValidationQueries_t*));
					thread_array[i].size = max_validations;
				}
			}
			Val_list_node* iter = validation_list->list->list_beg;
			
			for(i = 0; i < thread_num; i++){
				thread_array[i].validation_num = 0;	
				thread_array[i].result_array = result_array;
				thread_array[i].journal_array = journal_array;
				thread_array[i].first_val_id = iter->data->validationId;
			}
			/*Assign validation pointers to thread arguements*/
			uint64_t index = 0;
			while(iter != NULL && iter->data->validationId < fl->validationId){
				ValidationQueries_t* val_query = iter->data;
				uint64_t validation_num = thread_array[index].validation_num;
				thread_array[index].validation_array[validation_num] = val_query;
				thread_array[index].validation_num++;
				if(++index == thread_num){
					index = 0;
				}
				iter = iter->next;
				remove_count++;
			}

			pthread_t* thread_id = malloc(thread_num*sizeof(pthread_t));
			ALLOCATION_ERROR(thread_id);

			int err;
			for(i=0; i<thread_num; i++){
				if( (err = pthread_create(&thread_id[i], NULL, threadFunction, &thread_array[i])) != 0){
					fprintf(stderr, "Error in pthread_create \n");
					fprintf(stderr, "\tError: %s\n",strerror(err));
					exit(EXIT_FAILURE);
				}
			}

			for(i=0; i<thread_num; i++){
				if( (err = pthread_join(thread_id[i], NULL ))!= 0){
					fprintf(stderr, "Error in pthread_join \n");
					fprintf(stderr, "\tError: %s\n",strerror(err));
					exit(EXIT_FAILURE);
				}
			}

			/*Remove Validations from validation list*/
			for(i = 0; i < remove_count; i++){
				validation_remove_start(validation_list->list);
				printf("%" PRIu8 "",result_array[i]);
			}
			last_validation_id = fl->validationId + 1;
			free(thread_id);
			free(result_array);
		} 
	} else {
		Val_list_node* iter = validation_list->list->list_beg;
		while(iter != NULL && iter->data->validationId < fl->validationId){
			ValidationQueries_t* val_query = iter->data;
			printf("%d", checkValidation(journal_array, val_query));
			iter = iter->next;
			validation_remove_start(validation_list->list);
		}
	}
}


void processForget(Forget_t *fo, Journal_t** journal_array,int relation_count) {
	return;
	uint32_t i;
	for(i = 0; i < relation_count; i++){
		Journal_t* journal = journal_array[i];
		forgetJournal(journal, fo->transactionId);
	}
}

void forgetJournal(Journal_t* journal, uint64_t transactionId) {
	if ( journal->predicate_index != NULL) {
		forgetPredicateIndex(journal->predicate_index,transactionId);
	}
}

bool checkValidation(Journal_t** journal_array, ValidationQueries_t* val_query){
	bool result = false;
	uint64_t i;
	const char* reader = val_query->queries;
	for(i = 0; i < val_query->queryCount; i++){
		Query_t* query = (Query_t*)reader;
		Journal_t* journal = journal_array[query->relationId];
		bool partial_result;
		if(journal->predicate_index != NULL)
			partial_result = checkQueryHash(journal_array, query, val_query->from, val_query->to);
		else
			partial_result = checkSingleQuery(journal_array, query, val_query->from, val_query->to);
		if(partial_result == true){	/*Short circuiting*/
			return true;
		}
		result = result || partial_result;
		reader += sizeof(Query_t) + (sizeof(Column_t) * query->columnCount);
	}
	return result;
}


bool checkQueryHash(Journal_t** journal_array, Query_t* query, uint64_t from, uint64_t to){
	uint64_t i,j;
	uint64_t first_offset, offset;
	Journal_t* journal = journal_array[query->relationId];
	BitSet_t* intersection = NULL;
	if(query->columnCount == 0){		/*Empty query*/
		if (getRecordCount(journal, from, to, &first_offset) > 0){
			return true;
		} else {
			return false;
		}
	}
	bool records_unknown = true;
	RangeArray* range_array = NULL;
	uint64_t record_count = 0;
	uint64_t range_size = 0;
	for(i = 0; i < query->columnCount; i++) {
		Column_t* predicate = &query->columns[i];


		//If bit_set for this predicate has allready been computed, get it from the hash table.
		BitSet_t* predicate_bit_set = predicateGetBitSet(journal->predicate_index, from, to, predicate->column, predicate->op, predicate->value);

		if(predicate_bit_set == NULL){
			//Else compute it now.
			if(records_unknown == true){
				 record_count = getRecordCount(journal, from, to, &first_offset);
				 records_unknown = false;
			}
			predicate_bit_set = createBitSet(record_count);
			if(predicate->column == 0 && predicate->op == Equal){	/*If the predicate is like "C0 == ..."*/
				range_array = getHashRecord(journal->index, predicate->value, &range_size); /*Get records from hash table*/
				for(j = 0; j < range_size; j++){
					if(range_array[j].transaction_id > to)
						break;
					if(range_array[j].transaction_id >= from){
						uint64_t bit = range_array[j].rec_offset - first_offset;
						setBit(bit, predicate_bit_set);
					}
				}
			} else { /*Else check all the records in the range [from,to]*/
				for(j = 0, offset = first_offset; j < record_count; j++, offset++){
					JournalRecord_t* record = &journal->records[offset];
					// if(checkConstraint(record, predicate)){
					uint32_t column = predicate->column;
					Op_t operator = predicate->op;
					uint64_t value = predicate->value;
					bool cond = false;
					switch(operator){
						case Equal:
							cond = (record->column_values[column] == value);
							break;
						case NotEqual:
							cond = (record->column_values[column] != value);
							break;
						case Less:
							cond = (record->column_values[column] < value);
							break;
						case LessOrEqual:
							cond = (record->column_values[column] <= value);
							break;
						case Greater:
							cond = (record->column_values[column] > value);
							break;
						case GreaterOrEqual:
							cond = (record->column_values[column] >= value);
							break;
					}
					if(cond){
						setBit(j,predicate_bit_set);
					}
				}
			}

			//Insert bit_set in the hash table.
			predicateInsertBitSet(journal->predicate_index,from, to, predicate->column, predicate->op, predicate->value, predicate_bit_set);
		}
		if(i == 0) {
			intersection = createBitSet(predicate_bit_set->bit_size);	/*Bit set of the whole query*/
			copyBitSet(intersection, predicate_bit_set);
		} else {
			BitSet_t* previous_intersection = intersection;
			intersection = intersect(predicate_bit_set, previous_intersection);	/*interset with previous bit set*/
			destroyBitSet(previous_intersection);
			previous_intersection = NULL;
		}
		if(isBitSetEmpty(intersection)){
			destroyBitSet(intersection);
			return false;
		}
	}
	if(intersection != NULL){
		destroyBitSet(intersection);
	}
	return true;
}


bool checkSingleQuery(Journal_t** journal_array, Query_t* query, uint64_t from, uint64_t to){
	bool result = false; 
	Journal_t* journal = journal_array[query->relationId];
	uint64_t i, j, range_size = 0;
	RangeArray* range_array = NULL;
	if(query->columnCount > 0){
	/* check the first column of validation */
		if (query->columns[0].column == 0 && query->columns[0].op == Equal){ /* if primary key */
			range_array = getHashRecord(journal->index, query->columns[0].value, &range_size);
			if(range_array == NULL || range_size == 0){
				return false;
			}
		}
	}
	if (range_array != NULL) { 		/* we can now search range array */
		uint64_t first = 0;
		uint64_t last = range_size - 1;
		uint64_t middle = (first+last)/2;
		uint64_t first_appearance;
		bool not_found = false;
		while (first <= last && not_found == false) {
			if (range_array[middle].transaction_id < from){
				first = middle + 1;    
			} else if (range_array[middle].transaction_id == from) {
				first_appearance = middle;
				break;
			} else {
				if(middle == 0){
					not_found = true;
					break;
				}
				last = middle - 1;
			}
			middle = (first + last)/2;
		}
		if (first > last || not_found == true){	//Not found
			first_appearance = (last <= first) ? last : first;
			while(first_appearance < range_size && range_array[first_appearance].transaction_id < from){
				first_appearance++;
			}
		}
		if(first_appearance >= range_size){
			return false;
		}
		while(first_appearance > 0 && range_array[first_appearance-1].transaction_id == range_array[first_appearance].transaction_id){
			first_appearance--;
		}
		i = first_appearance;
		while(i < range_size && range_array[i].transaction_id <= to ) {				/* for i in range_array */
			uint64_t offset = range_array[i].rec_offset;
			JournalRecord_t* record = &journal->records[offset];
			bool record_result = true;
			for (j = 1 ; j < query->columnCount ; j++) { 	/* check all column constraints */
				Column_t* constraint = &query->columns[j];
				uint32_t column = constraint->column;
				Op_t operator = constraint->op;
				uint64_t value = constraint->value;
				bool cond = false;
				switch(operator){
					case Equal:
						cond = (record->column_values[column] == value);
						break;
					case NotEqual:
						cond = (record->column_values[column] != value);
						break;
					case Less:
						cond = (record->column_values[column] < value);
						break;
					case LessOrEqual:
						cond = (record->column_values[column] <= value);
						break;
					case Greater:
						cond = (record->column_values[column] > value);
						break;
					case GreaterOrEqual:
						cond = (record->column_values[column] >= value);
						break;
				}
				bool partial_result = cond;
				record_result = record_result && partial_result;
				if(partial_result == false){
					break;
				}
			}
			if(record_result == true){ /*Short circuiting*/
				return true;
			}
			result = result || record_result;
			i++;
		}
	} else { /* unfortunately we should search in whole range [from, to]*/
		uint64_t first_appearance = getJournalRecords(journal, from, to);
		i = first_appearance;
		while(i < journal->num_of_recs && journal->records[i].transaction_id <= to ) {
			JournalRecord_t* record = &journal->records[i];
			bool record_result = true;
			for(j = 0; j < query->columnCount; j++){
				Column_t* constraint = &query->columns[j];
				uint32_t column = constraint->column;
				Op_t operator = constraint->op;
				uint64_t value = constraint->value;
				bool cond = false;
				switch(operator){
					case Equal:
						cond = (record->column_values[column] == value);
						break;
					case NotEqual:
						cond = (record->column_values[column] != value);
						break;
					case Less:
						cond = (record->column_values[column] < value);
						break;
					case LessOrEqual:
						cond = (record->column_values[column] <= value);
						break;
					case Greater:
						cond = (record->column_values[column] > value);
						break;
					case GreaterOrEqual:
						cond = (record->column_values[column] >= value);
						break;
				}
				bool partial_result = cond;
				record_result = record_result && partial_result;
				if(partial_result == false){
					break;
				}
			}
			if(record_result == true){ /*Short circuiting*/
				return true;
			}
			result = result || record_result;
			i++;
		}
	}
	return result;
}

void destroySchema(Journal_t** journal_array, int relation_count){
	uint32_t i;
	for(i = 0; i< relation_count; i++){
		destroyJournal(journal_array[i]);
	}
	free(journal_array);
	free(schema);
	if(modes[2] != 0){
		for(i = 0; i < modes[2]; i++){
			free(thread_array[i].validation_array);
		}
		free(thread_array);
	}
}

ValidationList_t* validationListCreate(){
	ValidationList_t* validation_list = malloc(sizeof(ValidationList_t));
	ALLOCATION_ERROR(validation_list);
	validation_list->list = validation_list_create();
	return validation_list;
}

void validationListDestroy(ValidationList_t* validation_list){
	destroy_validation_list(validation_list->list);
	free(validation_list);
}

int validationListInsert(ValidationList_t* validation_list, ValidationQueries_t* val_query){
	validation_insert_end(validation_list->list, val_query);
	validation_list->num_of_validations++;
	return 0;
}

void validationListPrint(ValidationList_t* validation_list){
	validation_print_list(validation_list->list);
}

void printValidation(ValidationQueries_t* val_query){
	const char* reader = val_query->queries;
	// fprintf(stderr,"\nValidationQueries %lu [%lu, %lu] %u RESLUT: %d\n", val_query->validationId, val_query->from, val_query->to, val_query->queryCount,checkValidation(journal_array,val_query));
	fprintf(stderr,"\nValidationQueries %lu [%lu, %lu] %u\n", val_query->validationId, val_query->from, val_query->to, val_query->queryCount);
	uint32_t i,j;
	for (i = 0; i < val_query->queryCount; i++) {
		const Query_t* query = (Query_t*)reader;
		fprintf(stderr,"Query for relation %" PRIu32 " query columnCount = %d \n", query->relationId,query->columnCount);
		// fprintf(stderr,"Query for relation %" PRIu32 " query columnCount = %d RESULT: %d | max_tid: %zu\n", query->relationId,query->columnCount,checkSingleQuery(journal_array,query,val_query->from,val_query->to), journal_array[query->relationId]->records[journal_array[query->relationId]->num_of_recs-1].transaction_id);
		for(j = 0; j<query->columnCount; j++){
			const Column_t column = query->columns[j];
			printColumn(column);
		}
		reader += sizeof(Query_t) + (sizeof(Column_t) * query->columnCount);
	}
}

void printColumn(Column_t column){
	switch(column.op){
		case Equal:
			fprintf(stderr,"\tC%" PRIu32 " == %zu\n",column.column, column.value);
			break;
		case NotEqual:
			fprintf(stderr,"\tC%" PRIu32 " != %zu\n",column.column, column.value);
			break;
		case Less:
			fprintf(stderr,"\tC%" PRIu32 " < %zu\n",column.column, column.value);
			break;
		case LessOrEqual:
			fprintf(stderr,"\tC%" PRIu32 " <= %zu\n",column.column, column.value);
			break;
		case Greater:
			fprintf(stderr,"\tC%" PRIu32 " > %zu\n",column.column, column.value);
			break;
		case GreaterOrEqual:
			fprintf(stderr,"\tC%" PRIu32 " >= %zu\n",column.column, column.value);
			break;
		default:
			fprintf(stderr,"Wrong operator\n");
			exit(EXIT_FAILURE);
	}			
}


Val_list_t *validation_list_create(void) {
	Val_list_t *list = malloc(sizeof(Val_list_t));
	ALLOCATION_ERROR(list);
	list->list_beg = NULL;
	list->list_end = NULL;
	list->size = 0;
	return list;
}

void validation_insert_end(Val_list_t *list, ValidationQueries_t* val_query) {
	Val_list_node *n = malloc(sizeof(Val_list_node));
	ALLOCATION_ERROR(n);
	n->data = val_query;
	n->next = NULL;
	
	if (list->size == 0){
		list->list_beg = n;
		list->list_end = n;
	} else {
		list->list_end->next = n;
		list->list_end = n;
	}

	list->size++;
}

// void validation_remove_end(Val_list_t *list) {
// 	Val_list_node *n = list->list_end;
// 	destroyValQuery(n->data);
// 	if (n->prev == NULL)
// 		list->list_beg = n->next;
// 	else
// 		n->prev->next = n->next;
// 	if (n->next == NULL)
// 		list->list_end = n->prev;
// 	else
// 		n->next->prev = n->prev;
// 	list->size--;
// 	free(n);
// }

void validation_remove_start(Val_list_t *list) {
	Val_list_node *n = list->list_beg;
	if(n == NULL)
		return;
	list->list_beg = n->next;
	// destroyValQuery(n->data);
	free(n->data);
	n->data = NULL;
	free(n);
	list->size--;
	if(list->size == 0){
		list->list_beg = NULL;
		list->list_end = NULL;
	}
}



void destroy_validation_list(Val_list_t* list){
	while(!validation_isEmpty(list)){
		// validation_remove_end(list);
		validation_remove_start(list);
	}
	free(list);
}

bool validation_isEmpty(Val_list_t* list){
	return (list->size == 0);
}

void validation_print_list(Val_list_t* list){
	Val_list_node* n = list->list_beg;
	while(n != NULL){
		printValidation(n->data);
		n = n->next;
	}
	printf("\n");
}