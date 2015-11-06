#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include "extendibleHashing.h"
#include "parser.h"
#include "journal.h"

#define BUFF_SIZE 1024

static uint32_t* schema = NULL;

static void processDefineSchema(DefineSchema_t *s) {
	int i;
	printf("DefineSchema %d |", s->relationCount);
	if (schema == NULL)
		free(schema);
	schema = malloc(sizeof(uint32_t)*s->relationCount);
	for (i = 0; i < s->relationCount; i++) {
		printf(" %d ",s->columnCounts[i]);
		schema[i] = s->columnCounts[i];
	}
	printf("\n");
}

static void processTransaction(Transaction_t *t) {
	int i;
	const char* reader = t->operations;
	printf("Transaction %lu (%u, %u) |", t->transactionId, t->deleteCount, t->insertCount);
	for (i=0; i < t->deleteCount; i++) {
		const TransactionOperationDelete_t* o = (TransactionOperationDelete_t*)reader;
		printf("opdel rid %u #rows %u ", o->relationId, o->rowCount);
		reader+=sizeof(TransactionOperationDelete_t)+(sizeof(uint64_t)*o->rowCount);
	}
	printf(" \t| ");
	for (i=0; i < t->insertCount; i++) {
		const TransactionOperationInsert_t* o = (TransactionOperationInsert_t*)reader;
		printf("opins rid %u #rows %u |", o->relationId, o->rowCount);
		reader+=sizeof(TransactionOperationInsert_t)+(sizeof(uint64_t)*o->rowCount*schema[o->relationId]);
	}
	printf("\n");

}

static void processValidationQueries(ValidationQueries_t *v) {
	printf("ValidationQueries %lu [%lu, %lu] %u\n", v->validationId, v->from, v->to, v->queryCount);
}

static void processFlush(Flush_t *fl) {
	printf("Flush %lu\n", fl->validationId);
}

static void processForget(Forget_t *fo) {
	printf("Forget %lu\n", fo->transactionId);
}


int main(int argc, char **argv) {
	MessageHead_t head;
	void *body = NULL;
	uint32_t len;
	while (1) {
		/* Retrieve the message head */
		if (read(0, &head, sizeof(head)) <= 0) {
			exit(EXIT_FAILURE);
		} // crude error handling, should never happen
		printf("HEAD LEN %u \t| HEAD TYPE %u \t| DESC ", head.messageLen, head.type);

		/* Retrieve the message body */
		if (body != NULL)
			free(body);
		if (head.messageLen > 0 ) {
			body = malloc(head.messageLen * sizeof(char));
			if (read(0, body, head.messageLen) <= 0) { 
				fprintf(stderr, "Error in Read Body");
				exit(EXIT_FAILURE);
			} // crude error handling, should never happen
			len -= (sizeof(head) + head.messageLen);
		}

		// And interpret it
		switch (head.type) {
			case 
				Done: printf("\n");
				return 0;
			case 
				DefineSchema: processDefineSchema(body);
				break;
			case 
				Transaction: processTransaction(body);
				break;
			case 
				ValidationQueries: processValidationQueries(body);
				break;
			case 
				Flush: processFlush(body);
				break;
			case 
				Forget: processForget(body);
				break;
			default: 
				exit(EXIT_FAILURE);	// crude error handling, should never happen
		}
	}

	return 0;
}



// int main (int argc, char** argv) {

// 	/* Read from stdin */
// 	char *buffer = malloc(1024 * sizeof(char));
// 	ALLOCATION_ERROR(buffer);

// 	while (read(STDIN_FILENO, buffer, 1024) > 0) {
// 		// printf("%s\n\n", buffer);
// 		while (1) {
// 			char *name = getFirstToken(&buffer);
// 			skipWhiteSpaces(&buffer);

// 			DefineSchema_t *defineScema;
// 			if (!strcmp(name, "defineschema")) {
// 				printf("DEFINESCHEMA MAN MU!\n");
// 				defineScema = defineScemaParser(&buffer);
// 				uint32_t i;
// 				printf("Total relations: %d\nRelations count:\n", defineScema->relationCount);
// 				for (i = 0 ; i< defineScema->relationCount ; i++)
// 					printf("%d) %d\n", i, defineScema->columnCounts[i]);
// 			} else if (!strcmp(name, "transaction")) {
// 				printf("TRAANSACTION MAN MU!\n");
// 				Transaction_t *transaction = transactionParser(&buffer);
// 			} else if (!strcmp(name, "validation")) {
// 				// validationParser();
// 			} else if (!strcmp(name, "flush")) {
// 			} else if (!strcmp(name, "forget")) {
// 				printf("FORGET MAN MU!\n");
// 				Forget_t *forget = forgetParser(&buffer);
// 			} else if (!strcmp(name, "done")) {
// 			}
// 		}
		
// 	}

// }
