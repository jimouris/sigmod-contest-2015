#include "parser.h"
#include "extendibleHashing.h"

/* 	parses the input and if there is another "command" after it, returns the remaining in the buffer
*	example: definescema [3 4]\ntransaction blahblah
*	buffer after the return = "\ntransaction blahblah "
*/
DefineSchema_t* defineScemaParser(char **buffer) {
	char *buf = *buffer;
	while ((**buffer) != '\n' && (**buffer) != '\0')
		(*buffer)++;
	*(*buffer)++ = '\0';
	/* count Relations */
	char *pch = buf;
	uint32_t cnt = 0;
	while (*buf != ']') {
		if (*buf == ' ')
			cnt++;
		buf++;
	}
	cnt++;
	/* allocate appropriate space + for the flexible array */
	DefineSchema_t *defineScema = malloc(sizeof(DefineSchema_t) + cnt * sizeof(uint32_t));
	ALLOCATION_ERROR(defineScema);
	defineScema->relationCount = cnt;
	uint32_t i = 0;
	pch = strtok(pch, " ");
	while (pch != NULL) {
		if (i == 0) // skip '['
			pch++;
		if (i == cnt-1) { // skip ']'
			char *tmp = pch;
			while (*tmp != ']')
				tmp++;
			*tmp = '\0';
		}
		defineScema->columnCounts[i++] = atoi(pch);
		pch = strtok(NULL, " ");
	}
	return defineScema;
}

Transaction_t* transactionParser(char **buffer) {
	printf("buffer:->%s<-\n", *buffer);
	exit(0);
}

Forget_t* forgetParser(char **buffer) {
	Forget_t *forget = malloc(sizeof(Forget_t));
	skipWhiteSpaces(buffer);
	char *tid = getFirstToken(buffer);
	forget->transactionId = atoll(tid);
	return forget;
}