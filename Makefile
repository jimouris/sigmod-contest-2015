all:
	cd ./src; make
	mv ./src/a.out .

clean:
	cd ./src; make clean

