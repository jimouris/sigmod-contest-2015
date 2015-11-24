#!/bin/sh

make clean
make

echo "\nNow running!"
./a.out < inputs/small.bin > myout.test
echo "done :)\n"
gcc -o perlineprinter src/printperline.c
./perlineprinter < myout.test > myoutperline.test
./perlineprinter < outputs/small.out.bin > smalloutperline.test

diff myoutperline.test smalloutperline.test

rm myoutperline.test smalloutperline.test perlineprinter
make clean

