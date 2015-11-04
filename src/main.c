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

		char * pch;
		pch = strtok (buffer," ");
		while (pch != NULL) {
			if (!strcmp(buffer, "defineschema")) {
				// defineScemaParser();
			} else if (!strcmp(buffer, "transaction")) {
				// transactionParser();
			} else if (!strcmp(buffer, "validation")) {
				// validationParser();
			} else if (!strcmp(buffer, "flush")) {
			} else if (!strcmp(buffer, "forget")) {
			} else if (!strcmp(buffer, "done")) {
			}
			pch = strtok (NULL, " ");
		}


	}


}
