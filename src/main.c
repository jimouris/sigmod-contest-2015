#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include "PKeyHash.h"
#include "parser.h"
#include "journal.h"

bool* usage(int argc, char **argv) {
	int i;
	bool* modes = calloc(2, sizeof(bool));
	for (i = 1 ; i < argc ; i++) {
		if (!strcmp(argv[i], "-tid") || !strcmp(argv[i], "tid") || !strcmp(argv[i], "--tid")) {
			modes[0] = true;
		} else if (!strcmp(argv[i], "-predicate") || !strcmp(argv[i], "predicate") || !strcmp(argv[i], "--predicate")) {
			modes[1] = true;
		} else {
			fprintf(stderr, "Wrong Input! Run like:\n%s\nor\n%s --tid\nor\n%s --tid --predicate\n", argv[0], argv[0], argv[0]);
			exit(EXIT_FAILURE);
		}
	}
	return modes;
}

int main(int argc, char **argv) {
	bool* modes = usage(argc, argv); /* modes[0]: tid on/off, modes[1]: predicate on/off*/
	MessageHead_t head;
	void *body = NULL;
	uint32_t len;
	Journal_t** journal_array = NULL;
	ValidationList_t* validation_list = validationListCreate();
	int relation_count = 0;
	while (1) {
		/* Retrieve the message head */
		if (read(0, &head, sizeof(head)) <= 0) {
			exit(EXIT_FAILURE);
		} // crude error handling, should never happen
		// printf("HEAD LEN %u \t| HEAD TYPE %u \t| DESC ", head.messageLen, head.type);

		/* Retrieve the message body */
		// if (body != NULL)
		// 	free(body);
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
				// validationListPrint(validation_list);
				destroySchema(journal_array, relation_count);
				validationListDestroy(validation_list);
				free(modes);
				return EXIT_SUCCESS;
			case DefineSchema:
				journal_array = processDefineSchema(body, &relation_count, modes);
				if (body != NULL)
					free(body);
				break;
			case Transaction:
				processTransaction(body,journal_array);
				if (body != NULL)
					free(body);
				break;
			case ValidationQueries:
				processValidationQueries(body,journal_array,validation_list);
				break;
			case Flush:
				processFlush(body,journal_array,validation_list);
				if (body != NULL)
					free(body);
				break;
			case Forget:
				processForget(body,journal_array,relation_count);
				if (body != NULL)
					free(body);
				break;
			default: 
				exit(EXIT_FAILURE);	// crude error handling, should never happen
		}
	}

	

	return 0;
}

