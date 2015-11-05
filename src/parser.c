#include "parser.h"
#include "extendibleHashing.h"

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
		defineScema->columnCounts[i++] = atoll(pch);
		pch = strtok(NULL, " ");
	}
	return defineScema;
}

Transaction_t* transactionParser(char **buffer) {
	char *t_id = getFirstToken(buffer);
	skipWhiteSpaces(buffer);
	uint64_t transaction_id = atoll(t_id);
	fprintf(stderr,"TRANS_ID: %zd\n", transaction_id);
	//Deletes
	(*buffer)++;	//Skip the first '['
	int open_brackets = 1;
	while(((**buffer) != ']') && (open_brackets == 1)){
		skipWhiteSpaces(buffer);
		//Get Relation_id
		char *r_id = getFirstToken(buffer);
		skipWhiteSpaces(buffer);
		uint64_t relation_id = atoll(r_id);
		fprintf(stderr,"RID: %zd\n", relation_id);
		(*buffer)++;	//Skip the next '['
		open_brackets++;
		fprintf(stderr,"buffer:->%s<-\n", *buffer);
		//Get Column numbers
		while(**buffer != ']'){
			char* col_no = getFirstToken(buffer);
			skipWhiteSpaces(buffer);
			uint32_t column_number = atoll(col_no);
			fprintf(stderr,"COL_ID: %"PRIu32"\n", column_number);
			fprintf(stderr,"buffer:->%s<-\n", *buffer);
		}
		open_brackets--;












		(*buffer)++;
		
	}
	printf("DONE, open_brackets = %d\n",open_brackets);
	exit(0);
}

Forget_t* forgetParser(char **buffer) {
	Forget_t *forget = malloc(sizeof(Forget_t));
	skipWhiteSpaces(buffer);
	char *tid = getFirstToken(buffer);
	forget->transactionId = atoll(tid);
	return forget;
}
