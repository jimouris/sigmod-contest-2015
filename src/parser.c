
#include "parser.h"

static uint32_t* schema = NULL;


// void skipWhiteSpaces(char** str){
// 	while(isspace(**str)){
// 		(*str)++;
// 	}
// }

// //Also eats the next white character.
// char* getFirstToken(char** buffer){
// 	char *init = *buffer;
// 	int count = 0;
// 	while (!isspace(**buffer) && (**buffer != '[') && (**buffer != ']')){
// 		(*buffer)++;
// 		count++;
// 	}
// 	char* token = malloc((count+1) * sizeof(char));
// 	token = strncpy(token, init, count);
// 	token[count] = '\0';
// 	// fprintf(stderr, "COUNT: %d\n",count );
// 	// fprintf(stderr, "TOKEN: |%s|\n",token );
// 	return token;
// }


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
	// printf("Transaction %lu (%u, %u) |", t->transactionId, t->deleteCount, t->insertCount);
	for (i = 0; i < t->deleteCount; i++) {
		const TransactionOperationDelete_t* o = (TransactionOperationDelete_t*)reader;
		//Find the latest insert record.
		//Use the hash table
		//Insert the JournalRecord.
		// printf("opdel rid %u #rows %u ", o->relationId, o->rowCount);
		reader += sizeof(TransactionOperationDelete_t) + (sizeof(uint64_t) * o->rowCount);
	}
	// printf(" \t| ");
	for (i = 0; i < t->insertCount; i++) {
		const TransactionOperationInsert_t* o = (TransactionOperationInsert_t*)reader;

		JournalRecord_t* record = malloc(sizeof(JournalRecord_t));
		record->transaction_id = t->transactionId;
		record->columns = schema[o->relationId];
		record->column_values = malloc(record->columns * sizeof(uint64_t));
		for(j = 0; j < record->columns; j++){
			record->column_values[j] = o->values[j];
		}
		insertJournalRecord(journal_array[o->relationId], record);
		// printf("opins rid %u #rows %u |", o->relationId, o->rowCount);
		reader += sizeof(TransactionOperationInsert_t) + (sizeof(uint64_t) * o->rowCount * schema[o->relationId]);
	}
	// printf("\n");

}

void processValidationQueries(ValidationQueries_t *v, Journal_t** journal_array) {
	// printf("ValidationQueries %lu [%lu, %lu] %u\n", v->validationId, v->from, v->to, v->queryCount);
	int i;
	/*For each query*/
	const char* reader = v->queries;
	for (i = 0; i < v->queryCount; i++) {
		const Query_t* query = (Query_t*)reader;
		reader += sizeof(Query_t) + (sizeof(Column_t) * query->columnCount);
	}
}

void processFlush(Flush_t *fl, Journal_t** journal_array) {
	// printf("Flush %lu\n", fl->validationId);
}

void processForget(Forget_t *fo, Journal_t** journal_array) {
	// printf("Forget %lu\n", fo->transactionId);
}


