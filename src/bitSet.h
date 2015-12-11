#ifndef __BITSET__
#define __BITSET__
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <inttypes.h>
#include <limits.h>
#include "constants.h"



#define BITS2BYTES(nb) ((nb + CHAR_BIT - 1) / CHAR_BIT)

Boolean_t isBitSetEmpty(char*, uint64_t);
char* createBitSet(uint64_t, uint64_t*);
char* intersect(char*, char*, uint64_t);

void setBit(int,  char*);
int checkBit(int , char*);
void printBitSet(char*, int);

char *my_strrev(char *str);

// void printBinary(uint64_t);

#endif