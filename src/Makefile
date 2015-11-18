#
# In order to execute this "Makefile" just type "make"
#

OBJS 	= main.o extendibleHashing.o parser.o journal.o
SOURCE	= main.c extendibleHashing.c parser.c journal.c
HEADER  = extendibleHashing.h parser.h journal.h
OUT  	= a.out
CC		= gcc
FLAGS 	= -c -g -Wall
# -g option enables debugging mode 
# -c flag generates object code for separate files

all: $(OBJS)
	$(CC) $(OBJS) -o $(OUT) -O2

# create/compile the individual files >>separately<< 
main.o: main.c
	$(CC) $(FLAGS) main.c

extendibleHashing.o: extendibleHashing.c extendibleHashing.h 
	$(CC) $(FLAGS) extendibleHashing.c

parser.o: parser.c extendibleHashing.h 
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

