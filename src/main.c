#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include "extendibleHashing.h"
#include "parser.h"
#include "journal.h"


int main(int argc, char **argv) {
	MessageHead_t head;
	void *body = NULL;
	uint32_t len;
	Journal_t** journal_array = NULL;
	uint64_t relation_count = 0;
	while (1) {
		/* Retrieve the message head */
		if (read(0, &head, sizeof(head)) <= 0) {
			exit(EXIT_FAILURE);
		} // crude error handling, should never happen
		// printf("HEAD LEN %u \t| HEAD TYPE %u \t| DESC ", head.messageLen, head.type);

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
			case Done:
				printf("\n");
				int i;
				for(i = 0; i< relation_count; i++){
					printf("Journal No: %d\n",i );
					printJournal(journal_array[i]);
					printHash(journal_array[i]->index);
				}
				destroySchema(journal_array, relation_count);
				return 0;
			case DefineSchema:
				journal_array = processDefineSchema(body, &relation_count);
				break;
			case Transaction:
				processTransaction(body,journal_array);
				break;
			case ValidationQueries:
				processValidationQueries(body,journal_array);
				break;
			case Flush:
				processFlush(body,journal_array);
				break;
			case Forget:
				processForget(body,journal_array);
				break;
			default: 
				exit(EXIT_FAILURE);	// crude error handling, should never happen
		}
	}

	

	return 0;
}

