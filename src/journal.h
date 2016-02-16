#ifndef __JOURNAL__
#define __JOURNAL__
#include <inttypes.h>
#include <stdbool.h>
#include "PKeyHash.h"
#include "tidHash.h"
#include "predicateHash.h"

typedef struct pkHash pkHash;
typedef struct predicateHash predicateHash;

typedef struct Column {
  /// The column id
  uint32_t column;
  /// The operations
  Op_t op;
  /// The constant
  uint64_t value;
} Column_t;

typedef struct JournalRecord_t {
	uint64_t transaction_id;
	size_t columns;	//number of columns
	uint64_t* column_values;
	bool dirty_bit;
} JournalRecord_t;

typedef struct Journal_t {
	JournalRecord_t* records; 
	uint64_t num_of_recs;
	uint64_t journal_capacity;
	pkHash* index;
	tidHash* tid_index;
	predicateHash* predicate_index;
	uint64_t relation_id;
} Journal_t;

typedef struct List_node {
	JournalRecord_t* data;
	struct List_node *next;
	struct List_node *prev;
} List_node;

typedef struct List_t {
	List_node *list_beg;
	List_node *list_end;
	uint64_t size;
} List_t;

bool isEmpty(List_t*);
void printList(List_t*);
void destroy_list(List_t*);
bool checkConstraint(JournalRecord_t*, Column_t*);
List_node* insert_start(List_t* l_info, JournalRecord_t* d);
void insert_end(List_t* l_info, JournalRecord_t* d);
void remove_end(List_t* l_info);
List_t* info_init();
void print_list(List_t *l_info);
Journal_t* createJournal(uint64_t, int*);
void insertJournalRecord(Journal_t*, uint64_t, size_t, const uint64_t*, bool);
uint64_t getJournalRecords(Journal_t*, uint64_t range_start, uint64_t range_end);
int destroyJournal(Journal_t*);
int destroyJournalRecord(JournalRecord_t*);
void insertJournalRecordCopy(Journal_t* journal, JournalRecord_t* old, uint64_t, bool);
JournalRecord_t* copyJournalRecord(JournalRecord_t*);
int increaseJournal(Journal_t*);
void printJournal(Journal_t*);
void printJournalRecord(JournalRecord_t*);
uint64_t getRecordCount(Journal_t*, uint64_t, uint64_t, uint64_t*);
uint64_t findRangeEnd(Journal_t*, uint64_t, uint64_t, uint64_t, uint64_t*, uint64_t);
uint64_t findRangeStart(Journal_t*, uint64_t, uint64_t, uint64_t, uint64_t*, bool*);

#endif