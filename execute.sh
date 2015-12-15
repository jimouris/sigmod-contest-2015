#!/bin/sh

DEFAULTINPUT="./inputs/small.bin"
DEFAULTOUTPUT="./outputs/small.out.bin"

make clean
make
gcc -o perlineprinter src/printperline.c

echo "\n\nNow running!"
if [ $# -eq 0 ] ; then
    echo "No arguments supplied, running with $DEFAULTINPUT"
	time -f "\t%E Elapsed Real Time \n\t%S CPU-seconds" ./a.out < $DEFAULTINPUT > myout.test
	echo "done :)\n"
	./perlineprinter < myout.test > myoutperline.test
	rm myout.test
	./perlineprinter < $DEFAULTOUTPUT > $DEFAULTOUTPUT.perline.test
	diff myoutperline.test $DEFAULTOUTPUT.perline.test
	echo "\n\n"
else
	echo "reads from $1"
	input="${1}"
	if [ $# -eq 2 ] && [ "${2}" = "-tid" -o "${2}" = "--tid" -o "${2}" = "tid" ] ; then
		echo "tidHash is enabled"
		time -f "\t%E Elapsed Real Time \n\t%S CPU-seconds" ./a.out --tid < $input > myout.test
	elif [ $# -eq 2 ] && [ "${2}" = "-predicate" -o "${2}" = "--predicate" -o "${2}" = "predicate" ] ; then
		echo "predicateHash is enabled"
		time -f "\t%E Elapsed Real Time \n\t%S CPU-seconds" ./a.out --predicate < $input > myout.test
	elif [ $# -eq 3 ] ; then
		if [ "${2}" = "-tid" -o "${2}" = "--tid" -o "${2}" = "tid" ] && [ "${3}" = "-predicate" -o "${3}" = "--predicate" -o "${3}" = "predicate" ] ; then
			echo "Both predicateHash and tidHash is enabled"
			time -f "\t%E Elapsed Real Time \n\t%S CPU-seconds" ./a.out --tid --predicate < $input > myout.test
		elif  [ "${3}" = "-tid" -o "${3}" = "--tid" -o "${3}" = "tid" ] && [ "${2}" = "-predicate" -o "${2}" = "--predicate" -o "${2}" = "predicate" ] ; then
			echo "Both predicateHash and tidHash is enabled"
			time -f "\t%E Elapsed Real Time \n\t%S CPU-seconds" ./a.out --tid --predicate < $input > myout.test
		fi
	else
		# T="$(date +%s)"
		time -f "\t%E Elapsed Real Time \n\t%S CPU-seconds" ./a.out < $input > myout.test
	fi
	./perlineprinter < myout.test > myoutperline.test
	rm myout.test
	output="./outputs/"${input#*/}
	ending=${output##*.}
	output=${output%.*}".out."$ending
	# echo "$output"
	./perlineprinter < $output > $output.perline.test

	DIFF=$(diff myoutperline.test $output.perline.test)
	if [ "$DIFF" != "" ] ; then
	    echo "\n\nThe force wasn't with you my friend"
	    diff myoutperline.test $output.perline.test
	else
	    echo "\n\nThe force was with you this time, diff num of results 0!!"
	fi

	DEFAULTOUTPUT=$output
	echo "\n\n"
fi

rm myoutperline.test $DEFAULTOUTPUT.perline.test perlineprinter
