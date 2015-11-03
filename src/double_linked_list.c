#include "double_linked_list.h"

List_node *insert_start(List_t *l_info, int d) {
	List_node *n = malloc(sizeof(List_node));
	n->data = d;
	n->next = l_info->list_beg;
	n->prev = NULL;
	l_info->list_beg = n;
	if (n->next != NULL)
		n->next->prev = n;
	if (l_info->list_end == NULL)
		l_info->list_end = n;
	return n;
}

List_node *insert_end(List_t *l_info, int d) {
	List_node *n = malloc(sizeof(List_node));
	n->data = d;
	n->next = NULL;
	n->prev = l_info->list_end;
	l_info->list_end = n;
	if (n->prev != NULL)
		n->prev->next = n;
	if (l_info->list_beg == NULL)
		l_info->list_beg = n;
	return l_info->list_beg;
}

List_node *remove_end(List_t *l_info) {
	List_node *n = l_info->list_end;
	if (n->prev == NULL)
		l_info->list_beg = n->next;
	else
		n->prev->next = n->next;
	if (n->next == NULL)
		l_info->list_end = n->prev;
	else
		n->next->prev = n->prev;
	return n;
}

List_t *info_init(void) {
	List_t *l_info = malloc(sizeof(List_t));
	l_info->list_beg = NULL;
	l_info->list_end = NULL;
}

void print_list(List_t *l_info) {
	List_node *t = l_info->list_beg;
	while(t != NULL) {
		printf("%d ", t->data);
		t = t->next;
	}
	printf("\n");
}