#ifndef __CONSTANTS__
#define __CONSTANTS__ 

#define GLOBAL_DEPTH_INIT 1			/* Hash init depth */
#define B 1							/* Number of keys for each bucket */
#define C 4							/* Initial capacity for transactions in bucket */
#define JOURNAL_CAPACITY_INIT 64	


#define ALLOCATION_ERROR(X) if (X == NULL) { \
							perror("allocation error"); \
							exit(EXIT_FAILURE);} \



#endif
