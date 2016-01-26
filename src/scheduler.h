#ifndef THREADFUNCS_H_
#define THREADFUNCS_H_

#include <stdio.h>
#include <stdint.h>
#include <pthread.h>
#include <unistd.h>
#include <stdbool.h>
#include "parser.h"
#include "constants.h"

typedef struct thread_arg_t {
	ValidationQueries_t** validation_array;
	uint64_t size;
	uint64_t validation_num;
	uint8_t* result_array;
	Journal_t** journal_array;
	uint64_t first_val_id;
} thread_arg_t;

typedef struct job_node_t {
	ValidationQueries_t* v;
	struct job_node_t* next;
} job_node_t;

typedef struct job_queue {
	uint64_t jobs;
	job_node_t *list_start;
	job_node_t *list_end;
} job_queue;

typedef struct threadpool_t {
	pthread_t *threads;
	int thread_count;
	Journal_t** journal_array;
	uint8_t* result_array;
	uint64_t first_val_id;
	pthread_mutex_t lock;
	pthread_cond_t cond;
	job_queue *queue;
} threadpool_t;

void threadpoolBarrier(threadpool_t*);
job_queue* createQueue(void);
threadpool_t* threadpoolCreate(int, Journal_t**);
void* jobConsumer(void *);
void threadpoolAdd(threadpool_t *, ValidationQueries_t *);
void threadpoolFree(threadpool_t *);
void pushJob(job_queue *, ValidationQueries_t *);
ValidationQueries_t* popJob(job_queue *);
bool isQueueEmpty(job_queue *);

#endif