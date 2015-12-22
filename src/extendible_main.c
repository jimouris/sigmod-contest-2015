#include "PKeyHash.h"
#include <time.h>

int main(void)
{
	pkHash* myhash = createHash();
	
	// printf("hash->size: %llu , hash->global_depth: %zu\n",myhash->size,myhash->global_depth);
	RangeArray *rangeElement = malloc(1 *sizeof(RangeArray));
	rangeElement->transaction_id = 0;
	rangeElement->rec_offset = 100;
	insertHashRecord(myhash,3,rangeElement);
	insertHashRecord(myhash,6,rangeElement);
	insertHashRecord(myhash,7,rangeElement);
	insertHashRecord(myhash,8,rangeElement);
	insertHashRecord(myhash,10,rangeElement);
	insertHashRecord(myhash,12,rangeElement);
	insertHashRecord(myhash,15,rangeElement);
	insertHashRecord(myhash,3,rangeElement);
	insertHashRecord(myhash,8,rangeElement);

	printHash(myhash);
	// printf("It is %d \n",deleteHashRecord(myhash,61));
	destroyHash(myhash);
	free(rangeElement);
	return 0;
}