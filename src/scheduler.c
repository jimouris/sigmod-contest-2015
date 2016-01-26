#include "scheduler.h"

threadpool_t* threadpoolCreate(int thread_count) {
	threadpool_t* threadpool = malloc(sizeof(threadpool_t));
	ALLOCATION_ERROR(threadpool);
	threadpool->thread_count = thread_count;
	threadpool->threads = malloc(thread_count * sizeof(pthread_t));
	ALLOCATION_ERROR(threadpool->threads);
	if ((pthread_mutex_init(&(threadpool->lock), NULL) != 0) || (pthread_cond_init(&(threadpool->cond), NULL) != 0)) {
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

void threadpoolFree(threadpool_t *threadpool) {
    if (threadpool == NULL) {
        fprintf(stderr, "threadPool is null\n");
        exit(EXIT_FAILURE);
    }
    if (threadpool->threads != NULL) {
        free(threadpool->threads);
        pthread_mutex_destroy(&(threadpool->lock));
        pthread_cond_destroy(&(threadpool->cond));
    }
    free(threadpool);    
}

void pushJob(job_queue *queue, ValidationQueries_t* v) {
    job_node_t *node = malloc(sizeof(job_node_t));
    ALLOCATION_ERROR(node);
    node->v = v;
    node->next = NULL;
    if (queue->list_start == NULL) {
        queue->list_start = node;
        queue->list_end = node;
        return ;
    }
    queue->list_end->next = node;
    queue->list_end = node;
}

ValidationQueries_t* popJob(job_queue *queue) {
    job_node_t *node = queue->list_start;
    if (queue->list_start == NULL) {
        return NULL;
    }
    queue->list_start = queue->list_start->next;
    ValidationQueries_t* v = node->v;
    free(node);
    return v;
}

