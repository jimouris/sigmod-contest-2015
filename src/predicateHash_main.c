#include "predicateHash.h"
#include "bitSet.h"

int main(void)
{
	// predicateHash* myhash = predicateCreateHash();
	predicateSubBucket* predicateElement = malloc(sizeof(predicateSubBucket));
	predicateElement->condition = malloc(sizeof(Column_t));
	predicateElement->condition->column = 1;
	predicateElement->condition->op = 2;
	predicateElement->condition->value = 0;
	predicateElement->range_start = 1 ;
	predicateElement->range_end = 1 ;
	predicateElement->open_requests = 10 ;
	predicateElement->bit_set = createBitSet(10000);
	// predicateInsertHashRecord(myhash,predicateElement);
	predicateDestroySubBucket(predicateElement);
	free(predicateElement);
	// predicatePrintHash(myhash);
	// predicateDestroyHash(myhash);
	return 0;
}