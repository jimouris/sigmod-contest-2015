#include <stdio.h>
#include <stdlib.h>

typedef struct list_t {
	int data;
	struct list_t *next;
	struct list_t *prev;
} list_t;

typedef struct info {
	list_t *list_beg;
	list_t *list_end;
} info;

list_t *insert_start(info *l_info, int d) {
	list_t *n = malloc(sizeof(list_t));
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

list_t *insert_end(info *l_info, int d) {
	list_t *n = malloc(sizeof(list_t));
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

list_t *remove_end(info *l_info) {
	list_t *n = l_info->list_end;
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

info *info_init(void) {
	info *l_info = malloc(sizeof(info));
	l_info->list_beg = NULL;
	l_info->list_end = NULL;
}

void print_list(info *l_info) {
	list_t *t = l_info->list_beg;
	while(t != NULL) {
		printf("%d ", t->data);
		t = t->next;
	}
	printf("\n");
}

int main(void) {
	info *l_info = info_init();
	list_t *list = NULL;
	list = insert_end(l_info, 5);
	list = insert_start(l_info, 1);
	list = insert_end(l_info, 13);
	print_list(l_info);
	list = remove_end(l_info);
	list = insert_start(l_info, 12);
	print_list(l_info);

	return EXIT_SUCCESS;
}