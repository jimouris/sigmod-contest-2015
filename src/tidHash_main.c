#include "tidHash.h"

int main(void)
{
	tidHash* myhash = tidCreateHash();
	
	// printf("hash->size: %llu , hash->global_depth: %zu\n",myhash->size,myhash->global_depth);

	tidSubBucket* rangeElement = malloc(1 *sizeof(tidSubBucket));
	rangeElement->transaction_id = 0;
	rangeElement->rec_offset = 100;
	tidInsertHashRecord(myhash,rangeElement);
	rangeElement->transaction_id = 2;
	tidInsertHashRecord(myhash,rangeElement);
	rangeElement->transaction_id = 4;
	tidInsertHashRecord(myhash,rangeElement);
	rangeElement->transaction_id = 3;
	tidInsertHashRecord(myhash,rangeElement);
	rangeElement->transaction_id = 5;
	tidInsertHashRecord(myhash,rangeElement);
	
	tidPrintHash(myhash);
	// printf("It is %d \n",deleteHashRecord(myhash,61));
	tidDestroyHash(myhash);
	free(rangeElement);
	return 0;
}