#include "parser.h"
#include "extendibleHashing.h"

static uint32_t* schema = NULL;


void skipWhiteSpaces(char** str){
	while(isspace(**str)){
		(*str)++;
	}
}

//Also eats the next white character.
char* getFirstToken(char** buffer){
	char *init = *buffer;
	int count = 0;
	while (!isspace(**buffer) && (**buffer != '[') && (**buffer != ']')){
		(*buffer)++;
		count++;
	}
	char* token = malloc((count+1)*sizeof(char));
	token = strncpy(token, init, count);
	token[count] = '\0';
	// fprintf(stderr, "COUNT: %d\n",count );
	// fprintf(stderr, "TOKEN: |%s|\n",token );
	return token;
}


void processDefineSchema(DefineSchema_t *s) {
	int i;
	// printf("DefineSchema %d |", s->relationCount);
	if (schema == NULL)
		free(schema);
	schema = malloc(sizeof(uint32_t)*s->relationCount);
	for (i = 0; i < s->relationCount; i++) {
		// printf(" %d ",s->columnCounts[i]);
		schema[i] = s->columnCounts[i];
	}
	// printf("\n");
}

void processTransaction(Transaction_t *t) {
	int i;
	const char* reader = t->operations;
	// printf("Transaction %lu (%u, %u) |", t->transactionId, t->deleteCount, t->insertCount);
	for (i=0; i < t->deleteCount; i++) {
		const TransactionOperationDelete_t* o = (TransactionOperationDelete_t*)reader;
		// printf("opdel rid %u #rows %u ", o->relationId, o->rowCount);
		reader += sizeof(TransactionOperationDelete_t) + (sizeof(uint64_t)*o->rowCount);
	}
	// printf(" \t| ");
	for (i=0; i < t->insertCount; i++) {
		const TransactionOperationInsert_t* o = (TransactionOperationInsert_t*)reader;
		// printf("opins rid %u #rows %u |", o->relationId, o->rowCount);
		reader += sizeof(TransactionOperationInsert_t) + (sizeof(uint64_t)*o->rowCount*schema[o->relationId]);
	}
	// printf("\n");

}

void processValidationQueries(ValidationQueries_t *v) {
	// printf("ValidationQueries %lu [%lu, %lu] %u\n", v->validationId, v->from, v->to, v->queryCount);
	int i;
	/*For each query*/
	const char* reader = v->queries;
	for (i = 0; i < v->queryCount; i++) {
		const Query_t* query = (Query_t*)reader;
		reader += sizeof(Query_t) + (sizeof(Column_t) * query->columnCount);
	}
}

void processFlush(Flush_t *fl) {
	// printf("Flush %lu\n", fl->validationId);
}

void processForget(Forget_t *fo) {
	// printf("Forget %lu\n", fo->transactionId);
}
