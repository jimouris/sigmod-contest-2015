#
# In order to execute this "Makefile" just type "make"
#

OBJS 	= main.o PKeyHash.o tidHash.o predicateHash.o parser.o journal.o bitSet.o 
SOURCE	= main.c PKeyHash.c tidHash.c predicateHash.c parser.c journal.c bitSet.c
HEADER  = PKeyHash.h tidHash.h predicateHash.h parser.h journal.h bitSet.h constants.h
OUT  	= a.out
CC		= gcc
FLAGS 	= -c -g -Wall -O2
# -g option enables debugging mode 
# -c flag generates object code for separate files

all: $(OBJS)
	$(CC) $(OBJS) -o $(OUT) -O2

# create/compile the individual files >>separately<< 
main.o: main.c constants.h
	$(CC) $(FLAGS) main.c

PKeyHash.o: PKeyHash.c PKeyHash.h constants.h
	$(CC) $(FLAGS) PKeyHash.c

tidHash.o: tidHash.c tidHash.h constants.h
	$(CC) $(FLAGS) tidHash.c

predicateHash.o: predicateHash.c predicateHash.h constants.h
	$(CC) $(FLAGS) predicateHash.c

parser.o: parser.c PKeyHash.h constants.h
	$(CC) $(FLAGS) parser.c

journal.o: journal.c journal.h constants.h
	$(CC) $(FLAGS) journal.c

bitSet.o: bitSet.c constants.h
	$(CC) $(FLAGS) bitSet.c

# clean house
clean:
	rm -f $(OBJS) $(OUT)

clena:
	rm -f $(OBJS) $(OUT)

claen:
	rm -f $(OBJS) $(OUT)

