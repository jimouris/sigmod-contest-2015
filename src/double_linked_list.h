#ifndef __LIST__
#define __LIST__
#include <stdio.h>
#include <stdlib.h>


typedef struct List_node {
	int data;
	struct List_node *next;
	struct List_node *prev;
} List_node;

typedef struct List_t {
	List_node *list_beg;
	List_node *list_end;
} List_t;



List_node *insert_start(List_t *l_info, int d);

List_node *insert_end(List_t *l_info, int d);

List_node *remove_end(List_t *l_info);

List_t *info_init(void);

void print_list(List_t *l_info);

#endif