#include <stdio.h>
#include <stdlib.h>
#include "journal.h"

//List functions

List_node *insert_start(List_t *l_info, JournalRecord* d) {
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

List_node *insert_end(List_t *l_info, JournalRecord* d) {
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


//Journal functions


Journal* createJournal() {
	Journal* journal = malloc(sizeof(Journal));
	journal->journal_capacity = JOURNAL_CAPACITY_INIT;
	journal->records = malloc(journal->journal_capacity * sizeof(JournalRecord*));
	journal->num_of_recs = 0;
	return journal;
}

int insertJournalRecord(Journal* journal, JournalRecord* record) {
	if(journal->num_of_recs < journal->journal_capacity){
		journal->records[journal->num_of_recs] = record;
	} else {
		//Double the capacity
		//Insert the record
	}
	return 0;
}

//Den xerw ti kanei akrivws
//Den xerw pou xrhsimevei h JournalRecord* record. 
List_t* getJournalRecords(Journal* journal, JournalRecord* record, int range_start, int range_end) {
	int first = 0;
	int last = journal->num_of_recs - 1;
	int middle = (first+last)/2;
	int first_appearance;
	while (first <= last) {
		if (journal->records[middle]->transaction_id < range_start){
			first = middle + 1;    
		}
		else if (journal->records[middle]->transaction_id == range_start) {
			first_appearance = middle;
			break;
		}
		else{
			last = middle - 1;
		}
		middle = (first + last)/2;
	}
	if (first > last){	//Not found
		return NULL;
	}
	List_t* record_list = info_init();
	List_node* node = NULL;
	int i = first_appearance;
	while(i < journal->num_of_recs && journal->records[i]->transaction_id <= range_end ) {
		node = insert_end(record_list, journal->records[i]);
	}
	return record_list;
}

int destroyJournalRecord(JournalRecord* record){
	free(record->column_values);
	free(record);
	return 0;
}

int destroyJournal(Journal* journal) {
	int i;
	for(i=0; i<journal->num_of_recs; i++){
		destroyJournalRecord(journal->records[i]);
	}
	free(journal->records);
	free(journal);
	return 0;
}
