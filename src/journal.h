#ifndef __JOURNAL__
#define __JOURNAL__
#include <inttypes.h>
#include "extendibleHashing.h"


#define JOURNAL_CAPACITY_INIT 64




typedef struct JournalRecord_t {
	uint64_t transaction_id;
	size_t columns;	//number of columns
	uint32_t* column_values;
} JournalRecord_t;

typedef struct Journal_t {
	JournalRecord_t** records; /*To avoid offset fix pointers problem*/
	uint64_t num_of_recs;
	uint64_t journal_capacity;
} Journal_t;

typedef struct List_node {
	JournalRecord_t* data;
	struct List_node *next;
	struct List_node *prev;
} List_node;

typedef struct List_t {
	List_node *list_beg;
	List_node *list_end;
} List_t;



List_node* insert_start(List_t* l_info, JournalRecord_t* d);

List_node* insert_end(List_t* l_info, JournalRecord_t* d);

List_node* remove_end(List_t* l_info);

List_t* info_init();

void print_list(List_t *l_info);


Journal_t* createJournal();

int insertJournalRecord(Journal_t*, JournalRecord_t*);

List_t* getJournalRecords(Journal_t*, JournalRecord_t*, int range_start, int range_end);

int destroyJournal(Journal_t*);

int destroyJournalRecord(JournalRecord_t*);

JournalRecord_t* copyJournalRecord(JournalRecord_t*);


int increaseJournal(Journal_t*);


#endif