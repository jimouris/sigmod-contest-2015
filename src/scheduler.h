#ifndef THREADFUNCS_H_
#define THREADFUNCS_H_

#include <stdio.h>
#include <stdint.h>
#include <pthread.h>
#include <unistd.h>
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

typedef struct threadpool_t {
	pthread_t *threads;
	int thread_count;
	pthread_mutex_t lock;
} threadpool_t;

#endif