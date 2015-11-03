#
# In order to execute this "Makefile" just type "make"
#

OBJS 	= main.o extendibleHashing.o parser.o
SOURCE	= main.c extendibleHashing.c parser.c
HEADER  = extendibleHashing.h parser.h
OUT  	= a.out
CC		= gcc
FLAGS 	= -c
# -g option enables debugging mode 
# -c flag generates object code for separate files

all: $(OBJS)
	$(CC) $(OBJS) -o $(OUT) -O3

# create/compile the individual files >>separately<< 
main.o: main.c
	$(CC) $(FLAGS) main.c

extendibleHashing.o: extendibleHashing.c
	$(CC) $(FLAGS) extendibleHashing.c

parser.o: parser.c
	$(CC) $(FLAGS) parser.c

# clean house
clean:
	rm -f $(OBJS) $(OUT)
