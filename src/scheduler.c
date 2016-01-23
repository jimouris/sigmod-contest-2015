#include "scheduler.h"

threadpool_t* threadpoolCreate(int thread_count) {
	threadpool_t* threadpool = malloc(sizeof(threadpool_t));
	ALLOCATION_ERROR(threadpool);
	threadpool->thread_count = thread_count;
	threadpool->threads = malloc(thread_count * sizeof(pthread_t));
	ALLOCATION_ERROR(threadpool->threads);
	if (pthread_mutex_init(&(threadpool->lock), NULL) != 0) {
		fprintf(stderr, "Mutex allocation Error\n");
		exit(EXIT_FAILURE);
	}
	int i = 0;

/*edw 8elei allagh to create alla de 3erw me ti.*/
    for (i = 0; i < thread_count; i++) {
        if (pthread_create(&(threadpool->threads[i]), NULL, threadFunction, (void*)threadpool) != 0) {
        	fprintf(stderr, "Error creating thread %d\n", i);
        	exit(EXIT_FAILURE);
        }
    }

	return threadpool;
}

void threadpoolAdd(threadpool_t *threadpool, void (*function)(void *), thread_arg_t *argument) {
    if (pthread_mutex_lock(&(threadpool->lock)) != 0) {
    	fprintf(stderr, "Mutex locking Error\n");
		exit(EXIT_FAILURE);
    }





    if (pthread_mutex_unlock(&threadpool->lock) != 0) {
    	fprintf(stderr, "Mutex locking Error\n");
		exit(EXIT_FAILURE);
    }
}



