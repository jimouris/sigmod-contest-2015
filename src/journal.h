#ifndef __JOURNAL__
#define __JOURNAL__

#include "double_linked_list.h"

typedef struct JournalRecord {
	int transaction_id;
	int columns;	//number of columns
	int* column_values;
} JournalRecord;

typedef struct Journal {
	JournalRecord* records;
} Journal;

Journal* createJournal();

// OK_SUCCESS insertJournalRecord(Journal*, JournalRecord*, … );
int insertJournalRecord(Journal*, JournalRecord*);

// List<Record> getJournalRecords(Journal*, JournalRecord*, … , int range_start, int range_end);
List_t* getJournalRecords(Journal*, JournalRecord*, int range_start, int range_end);
// OK_SUCCESS destroyJournal(Journal*);
int destroyJournal(Journal*);

// OK_SUCCESS increaseJournal(Journal*, … );
int increaseJournal(Journal*);


#endif