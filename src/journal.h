// #ifndef __JOURNAL__
// #define __JOURNAL__

// #include "double_linked_list.h"


#define JOURNAL_CAPACITY_INIT 64




typedef struct JournalRecord {
	int transaction_id;
	size_t columns;	//number of columns
	int* column_values;
} JournalRecord;

typedef struct Journal {
	JournalRecord** records;
	size_t num_of_recs;
	size_t journal_capacity;
} Journal;

typedef struct List_node {
	// int data; //Change that
	JournalRecord* data;
	struct List_node *next;
	struct List_node *prev;
} List_node;

typedef struct List_t {
	List_node *list_beg;
	List_node *list_end;
} List_t;



List_node* insert_start(List_t* l_info, JournalRecord* d);

List_node* insert_end(List_t* l_info, JournalRecord* d);

List_node* remove_end(List_t* l_info);

List_t* info_init();

void print_list(List_t *l_info);


Journal* createJournal();

int insertJournalRecord(Journal*, JournalRecord*);

List_t* getJournalRecords(Journal*, JournalRecord*, int range_start, int range_end);

int destroyJournal(Journal*);

int increaseJournal(Journal*);


// #endif