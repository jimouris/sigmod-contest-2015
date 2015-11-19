#include <stdio.h>
#include <stdlib.h>
#include "journal.h"

/* List functions */

List_node *insert_start(List_t *l_info, JournalRecord_t* d) {
	List_node *n = malloc(sizeof(List_node));
	ALLOCATION_ERROR(n);
	JournalRecord_t* new_d = copyJournalRecord(d);
	n->data = new_d;
	n->next = l_info->list_beg;
	n->prev = NULL;
	l_info->list_beg = n;
	if (n->next != NULL)
		n->next->prev = n;
	if (l_info->list_end == NULL)
		l_info->list_end = n;
	l_info->size++;
	return n;
}

void insert_end(List_t *l_info, JournalRecord_t* d) {
	List_node *n = malloc(sizeof(List_node));
	ALLOCATION_ERROR(n);
	JournalRecord_t* new_d = copyJournalRecord(d);
	n->data = new_d;
	n->next = NULL;
	n->prev = l_info->list_end;
	l_info->list_end = n;
	if (n->prev != NULL)
		n->prev->next = n;
	if (l_info->list_beg == NULL)
		l_info->list_beg = n;
	l_info->size++;
}

void remove_end(List_t *l_info) {
	List_node *n = l_info->list_end;
	destroyJournalRecord(n->data);
	free(n->data);
	if (n->prev == NULL)
		l_info->list_beg = n->next;
	else
		n->prev->next = n->next;
	if (n->next == NULL)
		l_info->list_end = n->prev;
	else
		n->next->prev = n->prev;
	l_info->size--;
	free(n);
}

List_t *info_init(void) {
	List_t *l_info = malloc(sizeof(List_t));
	ALLOCATION_ERROR(l_info);
	l_info->list_beg = NULL;
	l_info->list_end = NULL;
	l_info->size = 0;
	return l_info;
}

void destroy_list(List_t* list){
	// uint64_t i;
	// uint64_t initial_size = list->size;
	// for(i = 0; i < initial_size; i++){
	// 	remove_end(list);
	// }
	while(!isEmpty(list)){
		remove_end(list);
	}
	free(list);
}

Boolean_t isEmpty(List_t* list){
	return (list->size == 0);
}


void printList(List_t* list){
	List_node* n = list->list_beg;
	while(n != NULL){
		printJournalRecord(n->data);
		n = n->next;
	}
	printf("\n");
}

/* Journal_t functions */

Journal_t* createJournal(uint64_t relation_id) {
	Journal_t* journal = malloc(sizeof(Journal_t));
	ALLOCATION_ERROR(journal);
	journal->journal_capacity = JOURNAL_CAPACITY_INIT;
	journal->records = malloc(journal->journal_capacity * sizeof(JournalRecord_t));
	ALLOCATION_ERROR(journal->records);
	journal->num_of_recs = 0;
	journal->relation_id = relation_id;
	journal->index = createHash();
	return journal;
}

int increaseJournal(Journal_t* journal){
	//Double the capacity
	journal->journal_capacity *= 2;
	journal->records = realloc(journal->records, journal->journal_capacity * sizeof(JournalRecord_t));
	ALLOCATION_ERROR(journal->records);
	return 0;
}

/*Returns a pointer to the inserted record*/

void insertJournalRecord(Journal_t* journal, uint64_t transaction_id, size_t columns, const uint64_t* column_values){
	if(journal->num_of_recs >= journal->journal_capacity) {
		increaseJournal(journal);
	}
	//Insert the record
	journal->records[journal->num_of_recs].transaction_id = transaction_id;
	journal->records[journal->num_of_recs].columns = columns;
	journal->records[journal->num_of_recs].column_values = malloc(journal->records[journal->num_of_recs].columns * sizeof(uint64_t));
	ALLOCATION_ERROR(journal->records[journal->num_of_recs].column_values);
	int i;
	for(i = 0; i < journal->records[journal->num_of_recs].columns; i++){
		journal->records[journal->num_of_recs].column_values[i] = column_values[i];
	}
	// record.dirty_bit = False;
	RangeArray* range_array = malloc(sizeof(RangeArray));
	ALLOCATION_ERROR(range_array);
	range_array->transaction_id = transaction_id;
	range_array->rec_offset = journal->num_of_recs;
	// printf("Relation: %zu, Key: %zu\n",journal->relation_id, column_values[0] );
	insertHashRecord(journal->index, column_values[0], range_array);
	free(range_array);
	journal->num_of_recs++;
}
// int insertJournalRecord(Journal_t* journal, JournalRecord_t* record) {
// 	if(journal->num_of_recs >= journal->journal_capacity) {
// 		increaseJournal(journal);
// 	}
// 	//Insert the record
// 	journal->records[journal->num_of_recs] = record;
// 	journal->num_of_recs++;
// 	// printf("Before insert hash\n");
// 	insertHashRecord(journal->index, record->column_values[0], NULL, record);
// 	// printf("After insert hash\n");
// 	return 0;
// }


/*
	In Columnt_t* constraint is the information for our constraint
	call it with NULL if you want all the records in the range.
*/
List_t* getJournalRecords(Journal_t* journal, Column_t* constraint, int range_start, int range_end) {
	/*Binary Search for first appearance*/
	uint64_t first = 0;
	uint64_t last = journal->num_of_recs - 1;
	uint64_t middle = (first+last)/2;
	uint64_t first_appearance;
	Boolean_t not_found = False;

	while (first <= last && not_found == False) {
		if (journal->records[middle].transaction_id < range_start){
			first = middle + 1;    
		}
		else if (journal->records[middle].transaction_id == range_start) {
			first_appearance = middle;
			break;
		}
		else{
			if(middle == 0){
				not_found = True;
				break;
			}
			last = middle - 1;
		}
		middle = (first + last)/2;
	}
	if (first > last || not_found == True){	//Not found
		first_appearance = (last <= first) ? last : first;
		while(first_appearance < journal->num_of_recs && journal->records[first_appearance].transaction_id < range_start){
			first_appearance++;
		}
	}
	List_t* record_list = info_init();
	if(first_appearance >= journal->num_of_recs){
		return record_list;
	}
	//////////////////
	while(first_appearance > 0 && journal->records[first_appearance-1].transaction_id == journal->records[first_appearance].transaction_id){
		first_appearance--;
	}
	//////////////////
	uint64_t i = first_appearance;
	while(i < journal->num_of_recs && journal->records[i].transaction_id <= range_end ) {
		JournalRecord_t* record = &journal->records[i];
		if(constraint == NULL || checkConstraint(record, constraint)){
			insert_end(record_list, record);
		}
		i++;
	}
	return record_list;
}

Boolean_t checkConstraint(JournalRecord_t* record, Column_t* constraint){
	uint32_t column = constraint->column;
	Op_t operator = constraint->op;
	uint64_t value = constraint->value;
	switch(operator){
		case Equal:
			return (record->column_values[column] == value);
		case NotEqual:
			return (record->column_values[column] != value);
		case Less:
			return (record->column_values[column] < value);
		case LessOrEqual:
			return (record->column_values[column] <= value);
		case Greater:
			return (record->column_values[column] > value);
		case GreaterOrEqual:
			return (record->column_values[column] >= value);
	}
	return False;
}

/*Returns a pointer to the inserted record*/
void insertJournalRecordCopy(Journal_t* journal, JournalRecord_t* old, uint64_t new_transaction_id){
	uint64_t transaction_id = new_transaction_id;
	size_t columns = old->columns;
	uint64_t* column_values = old->column_values;
	insertJournalRecord(journal, transaction_id, columns, column_values);
}

JournalRecord_t* copyJournalRecord(JournalRecord_t* old){
	JournalRecord_t* new_d = malloc(sizeof(JournalRecord_t));
	ALLOCATION_ERROR(new_d);
	new_d->transaction_id = old->transaction_id;
	new_d->columns = old->columns;
	uint64_t i;
	new_d->column_values = malloc(new_d->columns*sizeof(uint64_t));
	ALLOCATION_ERROR(new_d->column_values);
	for(i = 0; i < new_d->columns; i++){
		new_d->column_values[i] = old->column_values[i];
	}
	// new_d->dirty_bit = old->dirty_bit;
	return new_d;
}

int destroyJournalRecord(JournalRecord_t* record){
	free(record->column_values);
	// free(record);
	return 0;
}

int destroyJournal(Journal_t* journal) {
	int i;
	destroyHash(journal->index);
	for(i=0; i<journal->num_of_recs; i++){
		destroyJournalRecord(&journal->records[i]);
	}
	free(journal->records);
	free(journal);
	return 0;
}

void printJournalRecord(JournalRecord_t* rec) {
	size_t i;
	printf("Tr id: %zu\t",rec->transaction_id );
	for(i = 0; i < rec->columns; i++){
		printf("  %zu",rec->column_values[i]);
	}
	printf("\n\n");
}

void printJournal(Journal_t* journal){
	uint64_t i;
	for(i = 0; i < journal->num_of_recs; i++){
		printJournalRecord(&journal->records[i]);
	}
}

JournalRecord_t* createJournalRecord(uint64_t transaction_id, size_t columns, const uint64_t* column_values){
	uint64_t i;
	JournalRecord_t* record = malloc(sizeof(JournalRecord_t));
	ALLOCATION_ERROR(record);
	record->transaction_id = transaction_id;
	record->columns = columns;
	record->column_values = malloc(record->columns * sizeof(uint64_t));
	ALLOCATION_ERROR(record->column_values);
	for(i = 0; i < record->columns; i++){
		record->column_values[i] = column_values[i];
	}
	// record->dirty_bit = False;
	return record;
}



// void markDirty(JournalRecord_t* record) {
// 	record->dirty_bit = True;
// }
