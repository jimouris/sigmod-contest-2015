#include "predicateHash.h"

int main(void)
{
	predicateHash* myhash = predicateCreateHash();
	predicateSubBucket* predicateElement = malloc(1 *sizeof(predicateSubBucket));
	predicateElement->condition = malloc(sizeof(Column_t));
	predicateElement->condition->column = 1;
	predicateElement->condition->op = 2;
	predicateElement->condition->value = 0;
	predicateElement->range_start = 1 ;
	predicateElement->range_end = 1 ;
	predicateElement->conflict = -1;

	predicateInsertHashRecord(myhash,predicateElement);

	predicatePrintHash(myhash);
	predicateDestroyHash(myhash);
	free(predicateElement->condition);
	free(predicateElement);
	return 0;
}