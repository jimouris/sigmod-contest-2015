#ifndef __BITSET__
#define __BITSET__
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdbool.h>
#include <inttypes.h>
#include <limits.h>
#include "constants.h"



#define BITS2BYTES(nb) ((nb + CHAR_BIT - 1) / CHAR_BIT)


typedef struct BitSet_t {
	uint8_t* array;
	uint64_t bit_size;
} BitSet_t;

bool isBitSetEmpty(BitSet_t*);
BitSet_t* createBitSet(uint64_t);
BitSet_t* intersect(BitSet_t*, BitSet_t*);

void copyBitSet(BitSet_t*, BitSet_t*);

void setBit(int,  BitSet_t*);
int checkBit(int , BitSet_t*);
void printBitSet(BitSet_t*);

BitSet_t *my_strrev(BitSet_t *str);

void destroyBitSet(BitSet_t*);

// void printBinary(uint64_t);

#endif