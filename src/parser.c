#include "parser.h"

static uint32_t* schema = NULL;

Journal_t** processDefineSchema(DefineSchema_t *s, int *relation_count, Boolean_t* modes) {
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
			if (last_insertion != NULL && last_insertion->dirty_bit == False) {
				//Insert the JournalRecord.
				insertJournalRecordCopy(journal_array[o->relationId], last_insertion, t->transactionId, True); 
			} /* else { // the key doesn't exist, skip it } */
		}
		reader += sizeof(TransactionOperationDelete_t) + (sizeof(uint64_t) * o->rowCount);
	}
	for (i = 0; i < t->insertCount; i++) {
		const TransactionOperationInsert_t* o = (TransactionOperationInsert_t*)reader;
		for(j = 0; j < o->rowCount; j++){
			const uint64_t *values = o->values + j*schema[o->relationId];
			insertJournalRecord(journal_array[o->relationId], t->transactionId, schema[o->relationId], values, False);			
		}
		reader += sizeof(TransactionOperationInsert_t) + (sizeof(uint64_t) * o->rowCount * schema[o->relationId]);
	}
}


void processValidationQueries(ValidationQueries_t *v, Journal_t** journal_array, ValidationList_t* validation_list) {
	ValidationQueries_t* val_query = v;
	char* reader = v->queries;
	uint32_t i;
	for (i = 0; i < v->queryCount; i++) {
		Query_t* query = (Query_t*)reader;
		// uint32_t j;
		// for(j = 0; j<query->columnCount; j++){
		// 	if(query->columns[j].column == 0 && query->columns[j].op == Equal){
		// 		Column_t temp = query->columns[j];
		// 		query->columns[j] = query->columns[0];
		// 		query->columns[0] = temp;
		// 		break;
		// 	}
		// }
		qsort(query->columns, query->columnCount, sizeof(Column_t), cmp_col);
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

void processFlush(Flush_t *fl, Journal_t** journal_array, ValidationList_t* validation_list) {
	Val_list_node* iter = validation_list->list->list_beg;
	while(iter != NULL && iter->data->validationId < fl->validationId){
		ValidationQueries_t* val_query = iter->data;
		printf("%d", checkValidation(journal_array, val_query));
		// printValidation(val_query);
		iter = iter->next;
		validation_remove_start(validation_list->list);
	}
}

void processForget(Forget_t *fo, Journal_t** journal_array) {
	// Unimplemented;
	// printf("Forget %lu\n", fo->transactionId);
}

Boolean_t checkValidation(Journal_t** journal_array, ValidationQueries_t* val_query){
	Boolean_t result = False;
	uint64_t i;
	const char* reader = val_query->queries;
	for(i = 0; i < val_query->queryCount; i++){
		Query_t* query = (Query_t*)reader;
		Journal_t* journal = journal_array[query->relationId];
		Boolean_t partial_result;
		if(journal->predicate_index != NULL)
			partial_result = checkQueryHash(journal_array, query, val_query->from, val_query->to);
		else
			partial_result = checkSingleQuery(journal_array, query, val_query->from, val_query->to);
		if(partial_result == True){	/*Short circuiting*/
			return True;
		}
		result = result || partial_result;
		reader += sizeof(Query_t) + (sizeof(Column_t) * query->columnCount);
	}
	return result;
}


Boolean_t checkQueryHash(Journal_t** journal_array, Query_t* query, uint64_t from, uint64_t to){
	uint64_t i,j;
	uint64_t first_offset, offset;
	Journal_t* journal = journal_array[query->relationId];
	BitSet_t* intersection = NULL;
	if(query->columnCount == 0){		/*Empty query*/
		if (getRecordCount(journal, from, to, &first_offset) > 0){
			return True;
		} else {
			return False;
		}
	}
	Boolean_t records_unknown = True;
	RangeArray* range_array = NULL;
	uint64_t record_count = 0;
	uint64_t range_size = 0;
	for(i = 0; i < query->columnCount; i++) {
		Column_t* predicate = &query->columns[i];

		PredicateRangeArray* predicate_range_array = malloc(sizeof(PredicateRangeArray));
		ALLOCATION_ERROR(predicate_range_array);
		predicate_range_array->from = from;
		predicate_range_array->to = to;
		predicate_range_array->column = predicate->column;
		predicate_range_array->op = predicate->op;
		predicate_range_array->value = predicate->value;

		//If bit_set for this predicate has allready been computed, get it from the hash table.
		BitSet_t* predicate_bit_set = predicateGetBitSet(journal->predicate_index, predicate_range_array);
		free(predicate_range_array);

		if(predicate_bit_set == NULL){
			//Else compute it now.
			if(records_unknown == True){
				 record_count = getRecordCount(journal, from, to, &first_offset);
				 records_unknown = False;
			}
			predicateSubBucket* predicateSubBucket = createPredicateSubBucket(from, to, predicate->column, predicate->op, predicate->value);
			predicateSubBucket->bit_set = createBitSet(record_count);
			if(predicate->column == 0 && predicate->op == Equal){	/*If the predicate is like "C0 == ..."*/
				range_array = getHashRecord(journal->index, predicate->value, &range_size); /*Get records from hash table*/
				for(j = 0; j < range_size; j++){
					if(range_array[j].transaction_id > to)
						break;
					if(range_array[j].transaction_id >= from){
						uint64_t bit = range_array[j].rec_offset - first_offset;
						setBit(bit, predicateSubBucket->bit_set);
					}
				}
			} else { /*Else check all the records in the range [from,to]*/
				for(j = 0, offset = first_offset; j < record_count; j++, offset++){
					JournalRecord_t* record = &journal->records[offset];
					if(checkConstraint(record, predicate)){
						setBit(j,predicateSubBucket->bit_set);
					}
				}
			}

			predicate_bit_set = predicateSubBucket->bit_set;
			// predicate_bit_set = createBitSet(record_count);
			// copyBitSet(predicate_bit_set, predicateSubBucket->bit_set);

			//Insert predicate in the hash table.
			predicateInsertHashRecord(journal->predicate_index,predicateSubBucket);
		}


		// predicateDestroySubBucket(predicateSubBucket);
		// free(predicateSubBucket);
		

		if(i == 0) {
			intersection = createBitSet(predicate_bit_set->bit_size);	/*Bit set of the whole query*/
			copyBitSet(intersection, predicate_bit_set);
		} else {
			BitSet_t* previous_intersection = intersection;
			intersection = intersect(predicate_bit_set, previous_intersection);	/*interset with previous bit set*/
			// destroyBitSet(predicate_bit_set);
			// destroyBitSet(previous_intersection);
			// previous_intersection = NULL;
		}
		if(isBitSetEmpty(intersection)){
			destroyBitSet(intersection);
			return False;
		}
	}
	if(intersection != NULL){
		destroyBitSet(intersection);
	}
	return True;
}


Boolean_t checkSingleQuery(Journal_t** journal_array, Query_t* query, uint64_t from, uint64_t to){
	Boolean_t result = False; 
	Journal_t* journal = journal_array[query->relationId];
	uint64_t i, j, range_size = 0;
	RangeArray* range_array = NULL;
	if(query->columnCount > 0){
	/* check the first column of validation */
		if (query->columns[0].column == 0 && query->columns[0].op == Equal){ /* if primary key */
			range_array = getHashRecord(journal->index, query->columns[0].value, &range_size);
			if(range_array == NULL || range_size == 0){
				return False;
			}
		}
	}
	if (range_array != NULL) { 		/* we can now search range array */
		uint64_t first = 0;
		uint64_t last = range_size - 1;
		uint64_t middle = (first+last)/2;
		uint64_t first_appearance;
		Boolean_t not_found = False;
		while (first <= last && not_found == False) {
			if (range_array[middle].transaction_id < from){
				first = middle + 1;    
			} else if (range_array[middle].transaction_id == from) {
				first_appearance = middle;
				break;
			} else {
				if(middle == 0){
					not_found = True;
					break;
				}
				last = middle - 1;
			}
			middle = (first + last)/2;
		}
		if (first > last || not_found == True){	//Not found
			first_appearance = (last <= first) ? last : first;
			while(first_appearance < range_size && range_array[first_appearance].transaction_id < from){
				first_appearance++;
			}
		}
		if(first_appearance >= range_size){
			return False;
		}
		while(first_appearance > 0 && range_array[first_appearance-1].transaction_id == range_array[first_appearance].transaction_id){
			first_appearance--;
		}
		i = first_appearance;
		while(i < range_size && range_array[i].transaction_id <= to ) {				/* for i in range_array */
			uint64_t offset = range_array[i].rec_offset;
			JournalRecord_t* record = &journal->records[offset];
			Boolean_t record_result = True;
			for (j = 1 ; j < query->columnCount ; j++) { 	/* check all column constraints */
				Column_t* constraint = &query->columns[j];
				Boolean_t partial_result = checkConstraint(record,constraint);
				record_result = record_result && partial_result;
				if(partial_result == False){
					break;
				}
			}
			if(record_result == True){ /*Short circuiting*/
				return True;
			}
			result = result || record_result;
			i++;
		}
	} else { /* unfortunately we should search in whole range [from, to]*/
		uint64_t first_appearance = getJournalRecords(journal, from, to);
		i = first_appearance;
		while(i < journal->num_of_recs && journal->records[i].transaction_id <= to ) {
			JournalRecord_t* record = &journal->records[i];
			Boolean_t record_result = True;
			for(j = 0; j < query->columnCount; j++){
				Column_t* constraint = &query->columns[j];
				Boolean_t partial_result = checkConstraint(record,constraint);
				record_result = record_result && partial_result;
				if(partial_result == False){
					break;
				}
			}
			if(record_result == True){ /*Short circuiting*/
				return True;
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
			exit(1);
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

Boolean_t validation_isEmpty(Val_list_t* list){
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