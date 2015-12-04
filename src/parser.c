#include "parser.h"

static uint32_t* schema = NULL;

Journal_t** processDefineSchema(DefineSchema_t *s, int *relation_count) {
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
		journal_array[i] = createJournal(i);
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
	ValQuery_t* val_query = malloc(sizeof(ValQuery_t));
	ALLOCATION_ERROR(val_query);
	val_query->validationId = v->validationId;
	val_query->from = v->from;
	val_query->to = v->to;
	val_query->queryCount = v->queryCount;
	val_query->queries = malloc(val_query->queryCount*sizeof(SingleQuery_t*));
	ALLOCATION_ERROR(val_query->queries);

	const char* reader = v->queries;
	uint32_t i,j;
	for (i = 0; i < v->queryCount; i++) {
		const Query_t* query = (Query_t*)reader;
		val_query->queries[i] = malloc(sizeof(SingleQuery_t));
		ALLOCATION_ERROR(val_query->queries[i]);
		val_query->queries[i]->relationId = query->relationId;
		val_query->queries[i]->columnCount = query->columnCount;
		val_query->queries[i]->columns = malloc(query->columnCount*sizeof(Column_t*));
		ALLOCATION_ERROR(val_query->queries[i]->columns);

		for(j = 0; j<query->columnCount; j++){
			const Column_t column = query->columns[j];
			val_query->queries[i]->columns[j] = malloc(sizeof(Column_t));
			ALLOCATION_ERROR(val_query->queries[i]->columns[j]);
			val_query->queries[i]->columns[j]->column = column.column;
			val_query->queries[i]->columns[j]->op = column.op;
			val_query->queries[i]->columns[j]->value = column.value;
		}
		/* sort the columns to bring indexed c0 first */
		qsort(val_query->queries[i]->columns, val_query->queries[i]->columnCount, sizeof(Column_t*), cmp_col);

		reader += sizeof(Query_t) + (sizeof(Column_t) * query->columnCount);
	}
	validationListInsert(validation_list, val_query);
}

int cmp_col(const void *p1, const void *p2) {
	const Column_t *f1 = *(Column_t**) p1;
	const Column_t *f2 = *(Column_t**) p2;
	if (f1->column == f2->column) {
		if (f1->op == Equal) {
			return -1;
		} else if (f2->op == Equal) {
			return 1;
		} else
			return 0;
	} else {
		if(f1->column < f2->column){
			return -1;
		} else {
			return 1;
		}
	}	
}

void processFlush(Flush_t *fl, Journal_t** journal_array, ValidationList_t* validation_list) {
	Val_list_node* iter = validation_list->list->list_beg;
	while(iter != NULL && iter->data->validationId < fl->validationId){
		ValQuery_t* val_query = iter->data;
		printf("%d", checkValidation(journal_array, val_query));
		iter = iter->next;
		validation_remove_start(validation_list->list);
	}
}

void processForget(Forget_t *fo, Journal_t** journal_array) {
	// Unimplemented;
	// printf("Forget %lu\n", fo->transactionId);
}

Boolean_t checkValidation(Journal_t** journal_array, ValQuery_t* val_query){
	Boolean_t result = False;
	uint64_t i;
	for(i = 0; i < val_query->queryCount; i++){
		SingleQuery_t* query = val_query->queries[i];
		Boolean_t partial_result = checkSingleQuery(journal_array, query, val_query->from, val_query->to);
		if(partial_result == True){	/*Short circuiting*/
			return True;
		}
		result = result || partial_result;
	}
	return result;
}

Boolean_t checkSingleQuery(Journal_t** journal_array, SingleQuery_t* query, uint64_t from, uint64_t to){
	Boolean_t result = False; 
	Journal_t* journal = journal_array[query->relationId];
	uint64_t i, j, range_size = 0;
	RangeArray* range_array = NULL;
	if(query->columnCount > 0){
	/* check the first column of validation */
		if (query->columns[0]->column == 0 && query->columns[0]->op == Equal){ /* if primary key */
			range_array = getHashRecord(journal->index, query->columns[0]->value, &range_size);
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
				Column_t* constraint = query->columns[j];
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
		List_t* record_list = getJournalRecords(journal, NULL, from, to);
		List_node* iterator = record_list->list_beg;
		while(iterator != NULL){
			JournalRecord_t* record = iterator->data;
			Boolean_t record_result = True;
			for(j = 0; j < query->columnCount; j++){
				Column_t* constraint = query->columns[j];
				Boolean_t partial_result = checkConstraint(record,constraint);
				record_result = record_result && partial_result;
				if(partial_result == False){
					break;
				}
			}
			if(record_result == True){ /*Short circuiting*/
				destroy_list(record_list);
				return True;
			}
			result = result || record_result;
			iterator = iterator->next;
		}
		destroy_list(record_list);
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
	// validation_list->validation_array = malloc(VALIDATION_COUNT_INIT*sizeof(ValQuery_t*));
	// ALLOCATION_ERROR(validation_list->validation_array);
	// validation_list->num_of_validations = 0;
	// validation_list->capacity = VALIDATION_COUNT_INIT;
	validation_list->list = validation_list_create();
	return validation_list;
}

void validationListDestroy(ValidationList_t* validation_list){
	// uint64_t i;
	// for(i = 0; i < validation_list->num_of_validations; i++){
	// 	destroyValQuery(validation_list->validation_array[i]);
	// }
	// free(validation_list->validation_array);
	destroy_validation_list(validation_list->list);
	free(validation_list);
}

void destroyValQuery(ValQuery_t* val_query){
	uint64_t i;
	for(i = 0; i < val_query->queryCount; i++){
		destroySingleQuery(val_query->queries[i]);
	}
	free(val_query->queries);
	free(val_query);
}

void destroySingleQuery(SingleQuery_t* query){
	uint64_t i;
	for(i = 0; i< query->columnCount; i++){
		free(query->columns[i]);
	}
	free(query->columns);
	free(query);
}

int validationListInsert(ValidationList_t* validation_list, ValQuery_t* val_query){
	// if(validation_list->num_of_validations >= validation_list->capacity){
	// 	validation_list->capacity *= 2;
	// 	validation_list->validation_array = realloc(validation_list->validation_array, validation_list->capacity * sizeof(ValidationQueries_t));
	// 	ALLOCATION_ERROR(validation_list->validation_array);
	// }
	// validation_list->validation_array[validation_list->num_of_validations] = val_query;
	validation_insert_end(validation_list->list, val_query);
	validation_list->num_of_validations++;
	return 0;
}

void validationListPrint(ValidationList_t* validation_list, Journal_t ** journal_array){
	// uint64_t i;
	// for(i = 0; i < validation_list->num_of_validations; i++ ){
	// 	printValidation(validation_list->validation_array[i], journal_array);
	// }
	validation_print_list(validation_list->list, journal_array);
}

void printValidation(ValQuery_t* val_query, Journal_t** journal_array){
	fprintf(stderr,"\nValidationQueries %lu [%lu, %lu] %u RESLUT: %d\n", val_query->validationId, val_query->from, val_query->to, val_query->queryCount,checkValidation(journal_array,val_query));
	
	uint32_t i,j;
	/*For each query*/
	for (i = 0; i < val_query->queryCount; i++) {
		SingleQuery_t* query = val_query->queries[i];
		fprintf(stderr,"Query for relation %" PRIu32 " query columnCount = %d RESULT: %d | max_tid: %zu\n", query->relationId,query->columnCount,checkSingleQuery(journal_array,query,val_query->from,val_query->to), journal_array[query->relationId]->records[journal_array[query->relationId]->num_of_recs-1].transaction_id);

		for(j = 0; j<query->columnCount; j++){
			const Column_t* column = query->columns[j];
			switch(column->op){
				case Equal:
					fprintf(stderr,"\tC%" PRIu32 " == %zu\n",column->column, column->value);
					break;
				case NotEqual:
					fprintf(stderr,"\tC%" PRIu32 " != %zu\n",column->column, column->value);
					break;
				case Less:
					fprintf(stderr,"\tC%" PRIu32 " < %zu\n",column->column, column->value);
					break;
				case LessOrEqual:
					fprintf(stderr,"\tC%" PRIu32 " <= %zu\n",column->column, column->value);
					break;
				case Greater:
					fprintf(stderr,"\tC%" PRIu32 " > %zu\n",column->column, column->value);
					break;
				case GreaterOrEqual:
					fprintf(stderr,"\tC%" PRIu32 " >= %zu\n",column->column, column->value);
					break;
				default:
					fprintf(stderr,"Wrong operator\n");
					exit(1);
			}			
		}
	}
}


void validation_insert_end(Val_list_t *list, ValQuery_t* val_query) {
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
	// if(list->list_beg != NULL)
	// 	list->list_beg->prev = NULL;
	destroyValQuery(n->data);
	free(n);
	list->size--;
	if(list->size == 0){
		list->list_beg = NULL;
		list->list_end = NULL;
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

void validation_print_list(Val_list_t* list, Journal_t ** journal_array){
	Val_list_node* n = list->list_beg;
	while(n != NULL){
		printValidation(n->data, journal_array);
		n = n->next;
	}
	printf("\n");
}