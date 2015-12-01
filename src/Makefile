#
# In order to execute this "Makefile" just type "make"
#

OBJS 	= main.o PKeyHash.o parser.o journal.o
SOURCE	= main.c PKeyHash.c parser.c journal.c
HEADER  = PKeyHash.h parser.h journal.h
OUT  	= a.out
CC		= gcc
FLAGS 	= -c -g -Wall -O2
# -g option enables debugging mode 
# -c flag generates object code for separate files

all: $(OBJS)
	$(CC) $(OBJS) -o $(OUT) -O2

# create/compile the individual files >>separately<< 
main.o: main.c
	$(CC) $(FLAGS) main.c

PKeyHash.o: PKeyHash.c PKeyHash.h 
	$(CC) $(FLAGS) PKeyHash.c

parser.o: parser.c PKeyHash.h 
	$(CC) $(FLAGS) parser.c

journal.o: journal.c journal.h 
	$(CC) $(FLAGS) journal.c

# double_linked_list.o: double_linked_list.c
# 	$(CC) $(FLAGS) double_linked_list.c

# clean house
clean:
	rm -f $(OBJS) $(OUT)

clena:
	rm -f $(OBJS) $(OUT)

claen:
	rm -f $(OBJS) $(OUT)

