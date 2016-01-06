#include "predicateHash.h"
#include "bitSet.h"

int main(void)
{
	predicateHash* myhash = predicateCreateHash();
	
	predicateSubBucket* predicateElement = malloc(sizeof(predicateSubBucket));
	predicateElement->condition = malloc(sizeof(Column_t));
	predicateElement->condition->column = 1;
	predicateElement->condition->op = 2;
	predicateElement->condition->value = 0;
	predicateElement->range_start = 1 ;
	predicateElement->range_end = 1 ;
	predicateElement->open_requests = 10 ;
	predicateElement->bit_set = createBitSet(10000);
	predicateInsertHashRecord(myhash,predicateElement);


	predicateSubBucket* predicateElement2 = malloc(sizeof(predicateSubBucket));
	predicateElement2->condition = malloc(sizeof(Column_t));
	predicateElement2->condition->column = 1;
	predicateElement2->condition->op = 2;
	predicateElement2->condition->value = 0;
	predicateElement2->range_start = 1 ;
	predicateElement2->range_end = 2 ;
	predicateElement2->open_requests = 10 ;
	predicateElement2->bit_set = createBitSet(10000);
	predicateInsertHashRecord(myhash,predicateElement2);


	predicateSubBucket* predicateElement3 = malloc(sizeof(predicateSubBucket));
	predicateElement3->condition = malloc(sizeof(Column_t));
	predicateElement3->condition->column = 1;
	predicateElement3->condition->op = 2;
	predicateElement3->condition->value = 0;
	predicateElement3->range_start = 1 ;
	predicateElement3->range_end = 2 ;
	predicateElement3->open_requests = 10 ;
	predicateElement3->bit_set = createBitSet(10000);


	predicateDeleteHashRecord(myhash,predicateElement3);
	predicatePrintHash(myhash);
	// predicateDestroySubBucket(,predicateElement);
	//free(predicateElement);
	// predicatePrintHash(myhash);
	// predicateDestroyHash(myhash);
	return 0;
}