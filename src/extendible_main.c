#include "extendibleHashing.h"

int main(void)
{
	Hash* myhash = createHash();
	
	// printf("hash->size: %llu , hash->global_depth: %zu\n",myhash->size,myhash->global_depth);
	// printHash(myhash);
	insertHashRecord(myhash,2,NULL,800);
	insertHashRecord(myhash,4,NULL,801);
	insertHashRecord(myhash,6,NULL,802);
	// insertHashRecord(myhash,8,NULL,803);
	insertHashRecord(myhash,8,NULL,804);	
	// insertHashRecord(myhash,8,NULL,805);
	insertHashRecord(myhash,8,NULL,806);	
	// insertHashRecord(myhash,8,NULL,807);	
	 insertHashRecord(myhash,8,NULL,808);
	 // insertHashRecord(myhash,10,NULL,801);
	 // insertHashRecord(myhash,10,NULL,803);	
	 insertHashRecord(myhash,10,NULL,805);	
	 // insertHashRecord(myhash,10,NULL,817);
	 // insertHashRecord(myhash,10,NULL,819);

	 insertHashRecord(myhash,8,NULL,812);	
	 insertHashRecord(myhash,8,NULL,816);	
	 insertHashRecord(myhash,8,NULL,820);	
	 // insertHashRecord(myhash,8,NULL,818);
	 // insertHashRecord(myhash,8,NULL,819);	
	printHash(myhash);
	return 0;
}