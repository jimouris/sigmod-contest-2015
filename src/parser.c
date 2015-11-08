
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
	printf("Transaction %lu (%u, %u) |", t->transactionId, t->deleteCount, t->insertCount);
	for (i = 0; i < t->deleteCount; i++) {
		//Find the latest insert record.
		//Use the hash table
		//Insert the JournalRecord.
		const TransactionOperationDelete_t* o = (TransactionOperationDelete_t*)reader;
		for (j = 0; j < o->rowCount; j++){
			uint64_t key = o->keys[j];
			JournalRecord_t* last_insertion = getHashRecord2(journal_array[o->relationId]->index, key);
			if (last_insertion != NULL) {
				markDirty(last_insertion);
				JournalRecord_t* deletion = copyJournalRecord(last_insertion);
				deletion->transaction_id = t->transactionId;
				insertJournalRecord(journal_array[o->relationId], deletion); 
			}
			/* else { // the deletion doesn't exist } */
		}
		reader += sizeof(TransactionOperationDelete_t) + (sizeof(uint64_t) * o->rowCount);


		// printf("opdel rid %u #rows %u ", o->relationId, o->rowCount);
	}
	// printf(" \t| ");
	for (i = 0; i < t->insertCount; i++) {
		const TransactionOperationInsert_t* o = (TransactionOperationInsert_t*)reader;
		for(j = 0; j < o->rowCount; j++){
			const uint64_t *values = o->values + j*schema[o->relationId];
			int k;
			printf("----------------\n");
			for(k = 0; k<schema[o->relationId]; k++){
				printf("value: %zu\n",values[k] );
			}
			printf("----------------\n");
			JournalRecord_t* record = createJournalRecord(t->transactionId, schema[o->relationId], values);
			insertJournalRecord(journal_array[o->relationId], record);			
		}
		reader += sizeof(TransactionOperationInsert_t) + (sizeof(uint64_t) * o->rowCount * schema[o->relationId]);

		// printf("opins rid %u #rows %u |", o->relationId, o->rowCount);
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


