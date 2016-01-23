#include "scheduler.h"

threadpool_t* threadpoolCreate(int thread_count) {
	threadpool_t* threadpool = malloc(sizeof(threadpool_t));
	ALLOCATION_ERROR(threadpool);
	threadpool->threads = malloc(thread_count * sizeof(pthread_t));
	ALLOCATION_ERROR(threadpool->threads);
	if (pthread_mutex_init(&(threadpool->lock), NULL) != 0) {
		fprintf(stderr, "Mutex allocation Error\n");
		exit(EXIT_FAILURE);
	}
	threadpool->thread_count = thread_count;

	return threadpool;
}



