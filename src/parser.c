#include "parser.h"

static uint32_t* schema = NULL;

Journal_t** processDefineSchema(DefineSchema_t *s, int *relation_count) {
	int i;
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
		journal_array[i] = createJournal();
	}
	printf("\n");
	return journal_array;
}

void processTransaction(Transaction_t *t, Journal_t** journal_array) {
	uint64_t i,j;
	const char* reader = t->operations;
	printf("Transaction %lu (%u, %u)\n", t->transactionId, t->deleteCount, t->insertCount);
	for (i = 0; i < t->deleteCount; i++) {
		//Find the latest insert record.
		//Use the hash table
		//Insert the JournalRecord.
		const TransactionOperationDelete_t* o = (TransactionOperationDelete_t*)reader;
		for (j = 0; j < o->rowCount; j++){
			uint64_t key = o->keys[j];
			uint64_t last_offset = getLastOffset(journal_array[o->relationId]->index, key);
			JournalRecord_t* last_insertion = &journal_array[o->relationId]->records[last_offset];
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

void processFlush(Flush_t *fl, Journal_t** journal_array) {
	printf("Flush %lu\n", fl->validationId);
}

void processForget(Forget_t *fo, Journal_t** journal_array) {
	printf("Forget %lu\n", fo->transactionId);

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
	int i;
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