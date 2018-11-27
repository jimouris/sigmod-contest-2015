# transaction-processing [![License MIT][badge-license]](LICENSE.txt)
ACM Sigmod 2015 Programming Contest

> The ACID principle, mandating atomicity, consistency, isolation and durability is one of the cornerstones of relational database management systems. In order to achieve the third principle, isolation, concurrent reads and writes must be handled carefully.

> In practice, transaction processing is often done in an optimistic fashion, where queries and statements are processed first, and only at the commit point the system validates if there was a conflict between readers and writers.

> In this challenge, we simulate this in a slightly simplified way: The system processes already executed transactions and only needs to check efficiently whether concurrent queries conflict with them. So, in short: we issue a list of insert and delete statements and your program needs to figure out whether given predicates match the inserted or deleted data.

Website of the contest can be found [here](http://db.in.tum.de/sigmod15contest/task.html)

This repository contains the approach on this project by [Dimitris Mouris](https://github.com/jimouris), [Thanos Giannopoulos](https://github.com/thanosgn) and [Themis Beris](https://github.com/ThemisB).


### Running
Runnig of the program is easy using the script provided.


  ```
  ./execute.sh
  ```
  
The above will run the program with the default input file (`inputs/small.bin`).
It will also check if the output of the programm matches the output provided by the contest (using `diff`)
  
You can specify the input in the first arguement of the script, like:
  
```
./execute.sh input_file
```

After that can also follow some other options for using specific indexes, or enabling multihreaded functions.
  
##### Possible  options are:
  1. [`--tid` enables indexing for transaction-ids using extendible hashing]
  2. [`--predicate` enables indexing for predicates using extendible hashing, to avoid computing the same values twice]
  3. [`--threads t` enables multithreaded validation check using `t` threads]
  4. [`--rounds r` evaluates and print validadations every `r` flushes]
  5. [`--scheduler` enables thread scheduler for assigning jobs to a thread pool]


  
Unfortunately, for the given inputs options 1,2 don't provide speedup due to the overhead of building the indexes.
Also, regarding the multithreaded execution the scheduler doesn't provide a speedup either and the program runs faster with 
equal distribution of the validations to each thread.
  
#### Fastest Implementation:

```
./execute.sh input_file --threads 4
```

[badge-license]: https://img.shields.io/badge/license-MIT-green.svg?style=flat-square
