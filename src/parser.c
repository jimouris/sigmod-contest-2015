#include "parser.h"

static uint32_t* schema = NULL;

Journal_t** processDefineSchema(DefineSchema_t *s, int *relation_count) {
	uint64_t i;
	printf("DefineSchema %d |", s->relationCount);
	if (schema == NULL)
		free(schema);
	schema = malloc(sizeof(uint32_t) * s->relationCount);
	*relation_count = s->relationCount;
	ALLOCATION_ERROR(schema);
	Journal_t** journal_array = malloc(s->relationCount * sizeof(Journal_t*));
	ALLOCATION_ERROR(journal_array);
	for (i = 0; i < s->relationCount; i++) {
		printf(" %d ",s->columnCounts[i]);
		schema[i] = s->columnCounts[i];
		journal_array[i] = createJournal(i);
	}
	printf("\n");
	return journal_array;
}

void processTransaction(Transaction_t *t, Journal_t** journal_array) {
	uint64_t i,j;
	const char* reader = t->operations;
	// printf("Transaction %lu (%u, %u)\n", t->transactionId, t->deleteCount, t->insertCount);
	for (i = 0; i < t->deleteCount; i++) {
		//Find the latest insert record.
		//Use the hash table
		//Insert the JournalRecord.
		const TransactionOperationDelete_t* o = (TransactionOperationDelete_t*)reader;
		for (j = 0; j < o->rowCount; j++){
			uint64_t key = o->keys[j];
			JournalRecord_t* last_insertion = getLastRecord(journal_array[o->relationId], key);
			if (last_insertion != NULL) {
				// JournalRecord_t* deletion = copyJournalRecord(last_insertion);
				JournalRecord_t* deletion = insertJournalRecordCopy(journal_array[o->relationId], last_insertion); 
				deletion->transaction_id = t->transactionId;
			} /* else { // the deletion doesn't exist } */
		}
		reader += sizeof(TransactionOperationDelete_t) + (sizeof(uint64_t) * o->rowCount);
		// printf("opdel rid %u #rows %u ", o->relationId, o->rowCount);
	}
	for (i = 0; i < t->insertCount; i++) {
		const TransactionOperationInsert_t* o = (TransactionOperationInsert_t*)reader;
		for(j = 0; j < o->rowCount; j++){
			const uint64_t *values = o->values + j*schema[o->relationId];
			// JournalRecord_t* record = createJournalRecord(t->transactionId, schema[o->relationId], values);
			// insertJournalRecord(journal_array[o->relationId], record);			
			insertJournalRecord(journal_array[o->relationId], t->transactionId, schema[o->relationId], values);			
		}
		reader += sizeof(TransactionOperationInsert_t) + (sizeof(uint64_t) * o->rowCount * schema[o->relationId]);
		// printf("opins rid %u #rows %u |", o->relationId, o->rowCount);
	}
}

void processValidationQueries(ValidationQueries_t *v, Journal_t** journal_array, ValidationList_t* validation_list) {
	// fprintf(stderr, "Valid: %zu\n",v->validationId );
	ValQuery_t* val_query = malloc(sizeof(ValQuery_t));
	ALLOCATION_ERROR(val_query);
	val_query->validationId = v->validationId;
	val_query->from = v->from;
	val_query->to = v->to;
	val_query->queryCount = v->queryCount;
	val_query->queries = malloc(val_query->queryCount*sizeof(SingleQuery_t*));
	ALLOCATION_ERROR(val_query->queries);

	const char* reader = v->queries;
	int i,j;
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
		reader += sizeof(Query_t) + (sizeof(Column_t) * query->columnCount);
	}
	validationListInsert(validation_list, val_query);
}

void processFlush(Flush_t *fl, Journal_t** journal_array, ValidationList_t* validation_list) {
	static uint64_t current = 0;
	// printf("Flush %lu\n", fl->validationId);
	uint64_t i;
	for(i = current; i <= fl->validationId; i++){
		if(i < validation_list->num_of_validations){
			ValQuery_t* val_query = validation_list->validation_array[i];	
			// checkValidation(journal_array, val_query);
			printf("\tResult for ValID %zu is: %d\n",i,checkValidation(journal_array, val_query));
		}
	}
	current = fl->validationId;
}

void processForget(Forget_t *fo, Journal_t** journal_array) {
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
	Boolean_t result = True;
	Journal_t* journal = journal_array[query->relationId];
	uint64_t i;
	for(i = 0; i < query->columnCount; i++){
		Column_t* column = query->columns[i];
		Boolean_t partial_result = checkColumn(journal, column, from, to);
		if(partial_result == False){	/*Short circuiting*/
			return False;
		}
		result = result && partial_result;
	}
	return result;
}

Boolean_t checkColumn(Journal_t* journal,Column_t* column, uint64_t from, uint64_t to){
	Boolean_t  result;
	if(column->column == 0 && column->op == Equal){  /*Primary Key*/
		uint64_t range_size;
		RangeArray* range_array = getHashRecord(journal->index, column->value, &range_size);
		if(range_array == NULL){
			return False;
		}
		uint64_t i;
		Boolean_t exists = False;
		/*Binary Search for first appearance*/
		uint64_t first = 0;
		uint64_t last = range_size - 1;
		uint64_t middle = (first+last)/2;
		uint64_t first_appearance;
		while (first <= last ) {
			if (range_array[middle].transaction_id < from){
				first = middle + 1;    
			}
			else if (range_array[middle].transaction_id == from) {
				first_appearance = middle;
				break;
			}
			else{
				if(middle == 0){
					return False;
				}
				last = middle - 1;
			}
			middle = (first + last)/2;
		}
		if (first > last){	//Not found
			first_appearance = last;
			while(first_appearance < range_size && range_array[first_appearance].transaction_id < from){
				first_appearance++;
			}
		}

		i = first_appearance;
		while(i < range_size && range_array[i].transaction_id <= from ) {
			uint64_t offset = range_array[i].rec_offset;
			JournalRecord_t* record = &journal->records[offset];
			Boolean_t partial_result = checkConstraint(record, column);
			if(partial_result == True){
				return True;
			}
			exists = exists ||  partial_result;
			i++;
		}
		return exists;
	} else {
		List_t* record_list = getJournalRecords(journal, column, from, to);
		result = (!isEmpty(record_list));
		destroy_list(record_list);
	}
	return result;
}


void destroySchema(Journal_t** journal_array, int relation_count){
	int i;
	for(i = 0; i< relation_count; i++){
		destroyJournal(journal_array[i]);
	}
	free(journal_array);
	free(schema);
}

ValidationList_t* validationListCreate(){
	ValidationList_t* validation_list = malloc(sizeof(ValidationList_t));
	ALLOCATION_ERROR(validation_list);
	validation_list->validation_array = malloc(VALIDATION_COUNT_INIT*sizeof(ValQuery_t*));
	ALLOCATION_ERROR(validation_list->validation_array);
	validation_list->num_of_validations = 0;
	validation_list->capacity = VALIDATION_COUNT_INIT;
	return validation_list;
}

void validationListDestroy(ValidationList_t* validation_list){
	uint64_t i;
	for(i = 0; i < validation_list->num_of_validations; i++){
		destroyValQuery(validation_list->validation_array[i]);
	}
	free(validation_list->validation_array);
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
	if(validation_list->num_of_validations >= validation_list->capacity){
		validation_list->capacity *= 2;
		validation_list->validation_array = realloc(validation_list->validation_array, validation_list->capacity * sizeof(ValidationQueries_t));
		ALLOCATION_ERROR(validation_list->validation_array);
	}
	validation_list->validation_array[validation_list->num_of_validations] = val_query;
	validation_list->num_of_validations++;
	return 0;
}

void validationListPrint(ValidationList_t* validation_list){
	uint64_t i;
	for(i = 0; i < validation_list->num_of_validations; i++ ){
		printValidation(validation_list->validation_array[i]);
	}
}

void printValidation(ValQuery_t* val_query){
	printf("ValidationQueries %lu [%lu, %lu] %u\n", val_query->validationId, val_query->from, val_query->to, val_query->queryCount);
	
	int i,j;
	/*For each query*/
	for (i = 0; i < val_query->queryCount; i++) {
		const SingleQuery_t* query = val_query->queries[i];
		printf("Query for relation %" PRIu32 " query columnCount = %d\n", query->relationId,query->columnCount);

		for(j = 0; j<query->columnCount; j++){
			const Column_t* column = query->columns[j];
			switch(column->op){
				case Equal:
					printf("\tC%" PRIu32 " == %zu\n",column->column, column->value);
					break;
				case NotEqual:
					printf("\tC%" PRIu32 " != %zu\n",column->column, column->value);
					break;
				case Less:
					printf("\tC%" PRIu32 " < %zu\n",column->column, column->value);
					break;
				case LessOrEqual:
					printf("\tC%" PRIu32 " <= %zu\n",column->column, column->value);
					break;
				case Greater:
					printf("\tC%" PRIu32 " > %zu\n",column->column, column->value);
					break;
				case GreaterOrEqual:
					printf("\tC%" PRIu32 " >= %zu\n",column->column, column->value);
					break;
			}			
		}
	}
}