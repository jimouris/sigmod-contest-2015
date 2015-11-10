#include <stdio.h>
#include <stdlib.h>

typedef struct list_t {
    int data;
    struct list_t *next;
} list_t;

list_t *insert_start(list_t *list, int data) {
	list_t *node = malloc(sizeof(list_t));
	node->data = data;
	node->next = list;
	return node;
}

list_t *insert_end(list_t *list, int data) {
	list_t *node = malloc(sizeof(list_t));
	node->data = data;
	node->next = list;
	if (list == NULL)
		return node;
	list_t *temp = list;
	while (temp->next != NULL)
		temp = temp->next;
	temp->next = node;
	node->next = NULL;
	return list;
}

list_t *remove_first(list_t *list) {
    if (list == NULL)
    	return NULL;
    list_t *next = list->next;
    free(list);
    return next;
}

void print_list(list_t *list) {
	while (list != NULL) {
		printf("%d ", list->data);
		list = list->next;
	}
	printf("\n");
}

int main(void) {
	list_t *list = NULL;
	list = insert_start(list, 6);
	list = insert_start(list, 3);
	list = insert_start(list, 1);
	print_list(list);
	list = remove_first(list);
	print_list(list);
	list = insert_end(list, 12);
	list = insert_end(list, 13);
	list = insert_end(list, 14);
	print_list(list);
}