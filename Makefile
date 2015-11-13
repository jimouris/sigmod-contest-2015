all:
	cd ./src; make
	mv ./src/*.out .

clean:
	cd ./src; make clean
	rm ./*.out
