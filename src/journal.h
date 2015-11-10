#ifndef __JOURNAL__
#define __JOURNAL__
#include <inttypes.h>
#include "extendibleHashing.h"

typedef enum { False = 0, True = 1 } Boolean_t;

typedef struct Hash Hash;

typedef struct JournalRecord_t {
	uint64_t transaction_id;
	size_t columns;	//number of columns
	uint64_t* column_values;
	// Boolean_t dirty_bit;
} JournalRecord_t;

typedef struct Journal_t {
	JournalRecord_t* records; 
	uint64_t num_of_recs;
	uint64_t journal_capacity;
	Hash* index;
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

JournalRecord_t* insertJournalRecord(Journal_t*, uint64_t, size_t, const uint64_t*);
// int insertJournalRecord(Journal_t*, JournalRecord_t*);

List_t* getJournalRecords(Journal_t*, JournalRecord_t*, int range_start, int range_end);

int destroyJournal(Journal_t*);

int destroyJournalRecord(JournalRecord_t*);

JournalRecord_t* insertJournalRecordCopy(Journal_t* journal, JournalRecord_t* old);

JournalRecord_t* copyJournalRecord(JournalRecord_t*);


int increaseJournal(Journal_t*);

void printJournal(Journal_t*);

void printJournalRecord(JournalRecord_t*);

void markDirty(JournalRecord_t*);

JournalRecord_t* createJournalRecord(uint64_t, size_t, const uint64_t*);

#endif