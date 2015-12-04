#!/bin/sh

DEFAULTINPUT="./inputs/small.bin"
DEFAULTOUTPUT="./outputs/small.out.bin"

make clean
make
gcc -o perlineprinter src/printperline.c

echo "\n\nNow running!"
if [ $# -eq 0 ] ; then
    echo "No arguments supplied, running with $DEFAULTINPUT"
	./a.out < $DEFAULTINPUT > myout.test
	echo "done :)\n"
	./perlineprinter < myout.test > myoutperline.test
	rm myout.test
	./perlineprinter < $DEFAULTOUTPUT > $DEFAULTOUTPUT.perline.test
	diff myoutperline.test $DEFAULTOUTPUT.perline.test
	echo "\n\n"
else
	echo "running with $1"
	input="${1}"
	if [ $# -eq 2 ] && ( [ "${2}" == "--tid" ] || [ "${2}" == "-tid" ] || [ "${2}" == "tid" ] ); then
		echo "running with tidHash"
		./a.out --tid < $input > myout.test
	else
		./a.out < $input > myout.test
	fi
	echo "done :)\n"
	./perlineprinter < myout.test > myoutperline.test
	rm myout.test
	output="./outputs/"${input#*/}
	ending=${output##*.}
	output=${output%.*}".out."$ending
	# echo "$output"
	./perlineprinter < $output > $output.perline.test
	diff myoutperline.test $output.perline.test
	DEFAULTOUTPUT=$output
	echo "\n\n"
fi

rm myoutperline.test $DEFAULTOUTPUT.perline.test perlineprinter
make clean
