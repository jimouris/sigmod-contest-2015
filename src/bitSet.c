#include "bitSet.h"


// int main(){
// 	uint64_t byte_size;
// 	uint64_t transactions = 15;
// 	char* bit_set1 = createBitSet(transactions, &byte_size);
// 	char* bit_set2 = createBitSet(transactions, &byte_size);
// 	setBit(1, bit_set1);
// 	setBit(12, bit_set1);
// 	setBit(14, bit_set1);
	
// 	printBitSet(bit_set1,byte_size);

// 	setBit(14, bit_set2);
// 	setBit(5, bit_set2);
// 	setBit(0, bit_set2);
// 	printBitSet(bit_set2,byte_size);


// 	char* intersection = intersect(bit_set1, bit_set2, byte_size);


// 	printBitSet(intersection,byte_size);

// 	isBitSetEmpty(intersection, byte_size) ? printf("True\n") : printf("False\n");

// 	free(intersection);
// 	free(bit_set1);
// 	free(bit_set2);
// 	return 0;
// }


Boolean_t isBitSetEmpty(char* bit_set, uint64_t byte_size){
	uint64_t i = 0;
	for(i=0; i< byte_size; i++){
		if(bit_set[i] | 0){
			return False;
		}
	}
	return True;
}

/*
 * Creates a bitSet of 'bits' bits and stores in 'byte_size' the appropriate size in bytes.
 */
char* createBitSet(uint64_t bits, uint64_t* byte_size){
	uint64_t i;
	*byte_size = BITS2BYTES(bits);
	char* bit_set = malloc(*byte_size*sizeof(char));  // "byte_size" bytes
	memset(bit_set, 0, *byte_size);
	return bit_set;	
}


/*
 * Returns the intersection of 2 bitSets. (bis_set1 AND bit_set2)
 */
char* intersect(char* bit_set1, char* bit_set2, uint64_t byte_size){
	uint64_t i;
	char* bit_set = malloc(byte_size * sizeof(char));
	for(i=0; i< byte_size; i++){
		bit_set[i] = bit_set1[i] & bit_set2[i];
	}
	return bit_set;
}


/*
 * set nth bit to 1. (counting starts from 0)
 */
void setBit(int n,  char* bit_set){
	int pos = (n / CHAR_BIT);
	int bit = n % CHAR_BIT; 
	int num = 1 << (CHAR_BIT-1-bit);
	// printf("num = %d\n", num);
	bit_set[pos] |= num;
}

/*
 * get nth bit's value. (counting starts from 0)
 */
int checkBit(int n, char* bit_set){
	int pos = (n / CHAR_BIT);
	int bit = n % CHAR_BIT; 
	int num = bit_set[pos] >> (CHAR_BIT-1-bit);
	// printf("num = %d, result = %d\n", num, num & 1);
	return (num & 1);
}



void printBitSet(char* bit_set, int byte_size){
	int i,j,num;
	char* bin_rev;
	printf("| ");
	for(i=0; i<byte_size; i++){
		bin_rev = malloc(CHAR_BIT+1 * sizeof(char));
		strcpy(bin_rev,"");	
		num = bit_set[i];
		for(j=0; j<CHAR_BIT; j++){
			if(num & 1){
				strcat(bin_rev,"1");
			}else{
				strcat(bin_rev,"0");
			}
			num >>= 1;
		}
		printf("%s", my_strrev(bin_rev));


		printf(" | ");
		free(bin_rev);
	}
	printf("\n");
}

char *my_strrev(char *str)
{
      char *p1, *p2;

      if (! str || ! *str)
            return str;
      for (p1 = str, p2 = str + strlen(str) - 1; p2 > p1; ++p1, --p2)
      {
            *p1 ^= *p2;
            *p2 ^= *p1;
            *p1 ^= *p2;
      }
      return str;
}

// void printBinary(uint64_t n){
// 	if(!n)
//         printf("0");
// 	while (n) {
// 	    if (n & 1)
// 	        printf("1");
// 	    else
// 	        printf("0");

// 	    n >>= 1;
// 	}
// 	printf("\n");
// }