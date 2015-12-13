#ifndef __BITSET__
#define __BITSET__
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <inttypes.h>
#include <limits.h>
#include "constants.h"



#define BITS2BYTES(nb) ((nb + CHAR_BIT - 1) / CHAR_BIT)

Boolean_t isBitSetEmpty(uint8_t*, uint64_t);
uint8_t* createBitSet(uint64_t);
uint8_t* intersect(uint8_t*, uint8_t*, uint64_t);

void copyBitSet(uint8_t*, uint8_t*, uint64_t);

void setBit(int,  uint8_t*);
int checkBit(int , uint8_t*);
void printBitSet(uint8_t*, int);

uint8_t *my_strrev(uint8_t *str);

// void printBinary(uint64_t);

#endif