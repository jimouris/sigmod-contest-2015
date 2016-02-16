#include "bitSet.h"

bool isBitSetEmpty(BitSet_t* bit_set){
	uint64_t byte_size = BITS2BYTES(bit_set->bit_size);
	uint64_t i = 0;
	for(i=0; i< byte_size; i++){
		if(bit_set->array[i] | 0){
			return false;
		}
	}
	return true;
}

/*
 * Creates a bitSet of 'bit_size' bits.
 */
BitSet_t* createBitSet(uint64_t bit_size){
	uint64_t byte_size = BITS2BYTES(bit_size);
	// printf("bytes: %zu\n",byte_size );
	BitSet_t* bit_set = malloc(sizeof(BitSet_t));
	ALLOCATION_ERROR(bit_set);
	bit_set->bit_size = bit_size;

	bit_set->array = malloc(byte_size*sizeof(uint8_t));  // "byte_size" bytes
	ALLOCATION_ERROR(bit_set->array);
	memset(bit_set->array, 0, byte_size);
	return bit_set;	
}

void copyBitSet(BitSet_t* bit_set1, BitSet_t* bit_set2){
	bit_set1->bit_size = bit_set2->bit_size;
	uint64_t byte_size = BITS2BYTES(bit_set2->bit_size);
	memcpy(bit_set1->array, bit_set2->array, byte_size);
}

/*
 * Returns the intersection of 2 bitSets. (bis_set1 AND bit_set2)
 */
BitSet_t* intersect(BitSet_t* bit_set1, BitSet_t* bit_set2){
	uint64_t byte_size = BITS2BYTES(bit_set1->bit_size);
	uint64_t i;

	BitSet_t* bit_set = malloc(sizeof(BitSet_t));
	ALLOCATION_ERROR(bit_set);
	bit_set->array = malloc(byte_size * sizeof(uint8_t));
	ALLOCATION_ERROR(bit_set->array);
	bit_set->bit_size = bit_set1->bit_size;
	// fprintf(stderr, "size1: %zu \n",bit_set1->bit_size );
	// fprintf(stderr, "size2: %zu \n",bit_set2->bit_size );
	for(i=0; i< byte_size; i++){
		bit_set->array[i] = bit_set1->array[i] & bit_set2->array[i];
	}
	return bit_set;
}

/*
 * set nth bit to 1. (counting starts from 0)
 */
void setBit(int n,  BitSet_t* bit_set){
	int pos = (n / CHAR_BIT);
	int bit = n % CHAR_BIT; 
	int num = 1 << (CHAR_BIT-1-bit);
	// printf("num = %d\n", num);
	bit_set->array[pos] |= num;
}

/*
 * get nth bit's value. (counting starts from 0)
 */
int checkBit(int n, BitSet_t* bit_set){
	int pos = (n / CHAR_BIT);
	int bit = n % CHAR_BIT; 
	int num = bit_set->array[pos] >> (CHAR_BIT-1-bit);
	// printf("num = %d, result = %d\n", num, num & 1);
	return (num & 1);
}

void destroyBitSet(BitSet_t* bit_set){
	free(bit_set->array);
	free(bit_set);
}

void printBitSet(BitSet_t* bit_set){
	uint64_t byte_size = BITS2BYTES(bit_set->bit_size);
	uint64_t i,j;
	printf("| ");
	for(i = byte_size; i>0; i--){
		uint8_t num = bit_set->array[i-1];
		for(j=0; j<CHAR_BIT; j++){
			if(num & 1){
				printf("1");
			}else{
				printf("0");
			}
			num >>= 1;
		}
		printf(" | ");
	}
	printf("\n");
}
