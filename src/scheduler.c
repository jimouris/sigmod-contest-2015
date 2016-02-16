#include "scheduler.h"

threadpool_t* threadpoolCreate(int thread_count, Journal_t** journal_array) {
	threadpool_t* threadpool = malloc(sizeof(threadpool_t));
	ALLOCATION_ERROR(threadpool);
	threadpool->thread_count = thread_count;
	threadpool->threads = malloc(thread_count * sizeof(pthread_t));
	threadpool->journal_array = journal_array;
	ALLOCATION_ERROR(threadpool->threads);
	if ((pthread_mutex_init(&(threadpool->lock), NULL) != 0) || (pthread_cond_init(&(threadpool->cond), NULL) != 0)) {
		fprintf(stderr, "Mutex allocation Error\n");
		exit(EXIT_FAILURE);
	}
	if ((pthread_mutex_init(&(threadpool->lock_end), NULL) != 0) || (pthread_cond_init(&(threadpool->cond_end), NULL) != 0)) {
		fprintf(stderr, "Mutex allocation Error\n");
		exit(EXIT_FAILURE);
	}
	threadpool->queue = createQueue();
	int i = 0;
	for (i = 0; i < thread_count; i++) {
		if (pthread_create(&(threadpool->threads[i]), NULL, jobConsumer, threadpool) != 0) {
			fprintf(stderr, "Error creating thread %d\n", i);
			exit(EXIT_FAILURE);
		}
	}
	return threadpool;
}

void* jobConsumer(void *arg) {    
	while(1){
		threadpool_t * threadpool = (threadpool_t *) arg;
		if (pthread_mutex_lock(&(threadpool->lock)) != 0) {
			fprintf(stderr, "Mutex locking Error\n");
			exit(EXIT_FAILURE);
		}
		//Critical section start

		while (isQueueEmpty(threadpool->queue)) {
			pthread_cond_wait(&(threadpool->cond), &(threadpool->lock));
		}
		ValidationQueries_t* validation = popJob(threadpool->queue);
		if(checkValidation(threadpool->journal_array, validation)){
			uint64_t position = validation->validationId - threadpool->first_val_id;
			threadpool->result_array[position] = 1;
		}
		//Second CS
			if (pthread_mutex_lock(&(threadpool->lock_end)) != 0) {
				fprintf(stderr, "Mutex locking Error\n");
				exit(EXIT_FAILURE);
			}

			threadpool->num_of_validations--;
			if(threadpool->num_of_validations == 0)
				pthread_cond_signal(&(threadpool->cond_end));

			if (pthread_mutex_unlock(&threadpool->lock_end) != 0) {
				fprintf(stderr, "Mutex locking Error\n");
				exit(EXIT_FAILURE);
			}
		//Second CS

		//Critical section end
		if (pthread_mutex_unlock(&threadpool->lock) != 0) {
			fprintf(stderr, "Mutex locking Error\n");
			exit(EXIT_FAILURE);
		}
	}
	return NULL;
}

void threadpoolBarrier(threadpool_t* threadpool){
	if (pthread_mutex_lock(&(threadpool->lock_end)) != 0) {
		fprintf(stderr, "Mutex locking Error\n");
		exit(EXIT_FAILURE);
	}

	while (threadpool->num_of_validations > 0) {
		pthread_cond_wait(&(threadpool->cond_end), &(threadpool->lock_end));
	}

	if (pthread_mutex_unlock(&threadpool->lock_end) != 0) {
		fprintf(stderr, "Mutex locking Error\n");
		exit(EXIT_FAILURE);
	}
	return;      
}

/* scheduler producer */
void threadpoolAdd(threadpool_t *threadpool, ValidationQueries_t* v) {
	if (pthread_mutex_lock(&(threadpool->lock)) != 0) {
		fprintf(stderr, "Mutex locking Error\n");
		exit(EXIT_FAILURE);
	}

	pushJob(threadpool->queue, v);
	pthread_cond_broadcast(&(threadpool->cond));

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
	queue->jobs++;
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
	queue->jobs--;
	if (queue->jobs == 0) {
		queue->list_start = NULL;
	} else {
		queue->list_start = queue->list_start->next;
	}
	ValidationQueries_t* v = node->v;
	free(node);
	return v;
}

bool isQueueEmpty(job_queue *queue) {
	return (queue->jobs) ? false : true;
}

job_queue* createQueue(void) {
	job_queue* queue = malloc(sizeof(job_queue));
	ALLOCATION_ERROR(queue);
	queue->jobs = 0;
	queue->list_start = NULL;
	queue->list_end = NULL;
	return queue;
}
