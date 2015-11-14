#include "extendibleHashing.h"
#include <time.h>

int main(void)
{
	Hash* myhash = createHash();
	
	// printf("hash->size: %llu , hash->global_depth: %zu\n",myhash->size,myhash->global_depth);
	RangeArray **ranges = malloc(30 *sizeof(RangeArray *));
	int i;
	srand(time(NULL));
	for (i = 0 ; i < 16; i++) {
		ranges[i] = malloc(sizeof(RangeArray));
		ranges[i]->transaction_id = 20 + i;
		ranges[i]->rec_offset = rand() % 200;
		if (i == 0)
			insertHashRecord(myhash, 164, ranges[i]);
		else if (i == 1)
			insertHashRecord(myhash, 220, ranges[i]);
		else if (i == 2)
			insertHashRecord(myhash, 385, ranges[i]);
		else if (i == 3)
			insertHashRecord(myhash, 437, ranges[i]);
		else if (i == 4)
			insertHashRecord(myhash, 607, ranges[i]);
		else if (i == 5)
			insertHashRecord(myhash, 732, ranges[i]);
		else if (i == 6)
			insertHashRecord(myhash, 870, ranges[i]);
		else if (i == 7)
			insertHashRecord(myhash, 999, ranges[i]);
		else if (i == 8)
			insertHashRecord(myhash, 1010, ranges[i]);
		else if (i == 9)
			insertHashRecord(myhash, 1098, ranges[i]);
		else if (i == 10)
			insertHashRecord(myhash, 1154, ranges[i]);
		else if (i == 11)
			insertHashRecord(myhash, 1271, ranges[i]);
		else if (i == 12)
			insertHashRecord(myhash, 1427, ranges[i]);
		else if (i == 13)
			insertHashRecord(myhash, 1549, ranges[i]);
		else if (i == 14)
			insertHashRecord(myhash, 1607, ranges[i]);
		else if (i == 15)
			insertHashRecord(myhash, 1697, ranges[i]);
		// else 
			// insertHashRecord(myhash, 8, ranges[i]);
	}

	// for (i = 5 ; i < 14 ;i++) {
	// 	ranges[i] = malloc(sizeof(RangeArray));
	// 	ranges[i]->transaction_id = 20 + i;
	// 	ranges[i]->rec_offset = rand() % 200;
	// 	insertHashRecord(myhash,8,ranges[i]);
	// }

	// r1->transaction_id = 800;
	// r1->rec_offset = 43;
	// r2->transaction_id = 801;
	// r2->rec_offset = 43;
	// insertHashRecord(myhash, 4, r2);
	printHash(myhash);
	destroyHash(myhash);
	for (i = 0 ; i < 1 ; i++) {
		free(ranges[i]);
		ranges[i] = NULL;
	}
	free(ranges);
	// insertHashRecord(myhash,6,NULL,802);
	// // insertHashRecord(myhash,8,NULL,803);
	// insertHashRecord(myhash,8,NULL,804);	
	// // insertHashRecord(myhash,8,NULL,805);
	// insertHashRecord(myhash,8,NULL,806);	
	// // insertHashRecord(myhash,8,NULL,807);	
	//  insertHashRecord(myhash,8,NULL,808);
	//  // insertHashRecord(myhash,10,NULL,801);
	//  // insertHashRecord(myhash,10,NULL,803);	
	//  insertHashRecord(myhash,10,NULL,805);	
	//  // insertHashRecord(myhash,10,NULL,817);
	//  // insertHashRecord(myhash,10,NULL,819);

	//  insertHashRecord(myhash,8,NULL,812);	
	//  insertHashRecord(myhash,8,NULL,816);	
	//  insertHashRecord(myhash,8,NULL,820);	
	 // insertHashRecord(myhash,8,NULL,818);
	 // insertHashRecord(myhash,8,NULL,819);	
	// printHash(myhash);
	return 0;
}