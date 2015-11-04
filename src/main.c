#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include "extendibleHashing.h"
#include "parser.h"
#include "journal.h"

#define BUFF_SIZE 1024

int main (int argc, char** argv) {

	/* Read from stdin */
	char *buffer = malloc(1024 * sizeof(char));
	ALLOCATION_ERROR(buffer);

	while (read(STDIN_FILENO, buffer, 1024) > 0) {
		// printf("%s\n\n", buffer);
		char *name = buffer;
		while (*buffer != ' ')
			*buffer++;
		*buffer++ = '\0';
		if (!strcmp(name, "defineschema")) {
			printf("DEFINESCHEMA MAN MU!\n");
			DefineSchema_t *defineScema = defineScemaParser(buffer);
			uint32_t i;
			printf("Total relations: %d\nRelations count:\n", defineScema->relationCount);
			for (i = 0 ; i< defineScema->relationCount ; i++)
				printf("%d) %d\n", i, defineScema->columnCounts[i]);
		} else if (!strcmp(name, "transaction")) {
			// transactionParser();
			printf("TRAANSACTION MAN MU!\n");
		} else if (!strcmp(name, "validation")) {
			// validationParser();
		} else if (!strcmp(name, "flush")) {
		} else if (!strcmp(name, "forget")) {
		} else if (!strcmp(name, "done")) {
		}
	
	}

}
