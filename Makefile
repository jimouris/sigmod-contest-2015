#
# In order to execute this "Makefile" just type "make"
#

OBJS 	= main.o extendibleHashing.o
SOURCE	= main.cpp extendibleHashing.cpp
HEADER  = extendibleHashing.hpp
OUT  	= a.out
CC		= g++
FLAGS 	= -c
# -g option enables debugging mode 
# -c flag generates object code for separate files

all: $(OBJS)
	$(CC) $(OBJS) -o $(OUT) -O3

# create/compile the individual files >>separately<< 
main.o: main.cpp
	$(CC) $(FLAGS) main.cpp

extendibleHashing.o: extendibleHashing.cpp
	$(CC) $(FLAGS) extendibleHashing.cpp

# clean house
clean:
	rm -f $(OBJS) $(OUT)
