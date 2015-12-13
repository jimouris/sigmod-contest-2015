#include "bitSet.h"


// int main(){
// 	uint64_t transactions = 15;
// 	uint8_t* bit_set1 = createBitSet(transactions);
// 	uint8_t* bit_set2 = createBitSet(transactions);
// 	setBit(1, bit_set1);
// 	setBit(12, bit_set1);
// 	setBit(14, bit_set1);
	
// 	printBitSet(bit_set1,transactions);

// 	setBit(14, bit_set2);
// 	setBit(5, bit_set2);
// 	setBit(0, bit_set2);
// 	printBitSet(bit_set2,transactions);


// 	uint8_t* intersection = intersect(bit_set1, bit_set2, transactions);


// 	printBitSet(intersection,transactions);

// 	isBitSetEmpty(intersection, transactions) ? printf("True\n") : printf("False\n");

// 	free(intersection);
// 	free(bit_set1);
// 	free(bit_set2);
// 	return 0;
// }


Boolean_t isBitSetEmpty(uint8_t* bit_set, uint64_t bit_size){
	uint64_t byte_size = BITS2BYTES(bit_size);
	uint64_t i = 0;
	for(i=0; i< byte_size; i++){
		if(bit_set[i] | 0){
			return False;
		}
	}
	return True;
}

/*
 * Creates a bitSet of 'bit_size' bits.
 */
uint8_t* createBitSet(uint64_t bit_size){
	uint64_t i;
	uint64_t byte_size = BITS2BYTES(bit_size);
	printf("bytes: %zu\n",byte_size );
	uint8_t* bit_set = malloc(byte_size*sizeof(uint8_t));  // "byte_size" bytes
	memset(bit_set, 0, byte_size);
	return bit_set;	
}


/*
 * Returns the intersection of 2 bitSets. (bis_set1 AND bit_set2)
 */
uint8_t* intersect(uint8_t* bit_set1, uint8_t* bit_set2, uint64_t bit_size){
	uint64_t byte_size = BITS2BYTES(bit_size);
	uint64_t i;
	uint8_t* bit_set = malloc(byte_size * sizeof(uint8_t));
	for(i=0; i< byte_size; i++){
		bit_set[i] = bit_set1[i] & bit_set2[i];
	}
	return bit_set;
}


/*
 * set nth bit to 1. (counting starts from 0)
 */
void setBit(int n,  uint8_t* bit_set){
	int pos = (n / CHAR_BIT);
	int bit = n % CHAR_BIT; 
	int num = 1 << (CHAR_BIT-1-bit);
	// printf("num = %d\n", num);
	bit_set[pos] |= num;
}

/*
 * get nth bit's value. (counting starts from 0)
 */
int checkBit(int n, uint8_t* bit_set){
	int pos = (n / CHAR_BIT);
	int bit = n % CHAR_BIT; 
	int num = bit_set[pos] >> (CHAR_BIT-1-bit);
	// printf("num = %d, result = %d\n", num, num & 1);
	return (num & 1);
}



void printBitSet(uint8_t* bit_set, int bit_size){
	uint64_t byte_size = BITS2BYTES(bit_size);
	int i,j,num;
	uint8_t* bin_rev;
	printf("| ");
	for(i=0; i<byte_size; i++){
		bin_rev = malloc(CHAR_BIT+1 * sizeof(uint8_t));
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

uint8_t *my_strrev(uint8_t *str)
{
      uint8_t *p1, *p2;

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