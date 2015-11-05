#include <stdio.h>
#include <stdlib.h>
#include <string.h>

int main(void) {
	char *buf = malloc(1024 * sizeof(char));
	strcpy(buf, "defineschema [3 4]\ntransaction 3 [0 [3 4]] [0 [3 5 6]]\ntransaction 0 [] [\n0 [1 1 2   2 1 2   3 4 5   7 7 7]\n1 [1 0 0 0   3 0 0 1   4 1 1 1]\n]\ntransaction 1 [] [0 [6 5 4]]\ntransaction 2 [1 [4]]\nvalidation 0 1 2 [0 c0=4] [1 c1>8]\nvalidation 1 1 2 [1 c2=1]\nvalidation 2 1 3 [0 c0=3 c1=2] [0 c2=4]\n");
	FILE *fp = fopen("transactions.bin", "wb+");
	fwrite(buf, sizeof(char), 1024, fp);
	fclose(fp);
	return 0;
}