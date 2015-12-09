# transaction-processing
ACM Sigmod 2015 Programming Contest
http://db.in.tum.de/sigmod15contest/task.html

The ACID principle, mandating atomicity, consistency, isolation and durability is one of the cornerstones of relational database management systems. In order to achieve the third principle, isolation, concurrent reads and writes must be handled carefully.

In practice, transaction processing is often done in an optimistic fashion, where queries and statements are processed first, and only at the commit point the system validates if there was a conflict between readers and writers.

In this challenge, we simulate this in a slightly simplified way: The system processes already executed transactions and only needs to check efficiently whether concurrent queries conflict with them. So, in short: we issue a list of insert and delete statements and your program needs to figure out whether given predicates match the inserted or deleted data.

Each client has to process a number of requests, provided via standard input, and must provide answers via stdout. The protocol itself is a simple binary protocol, which is described in the C++ reference implementation. It also contains the testdriver and a basic test case to verify the functionality of your program locally. Additionally there are also a small and a medium-sized test case which can be used together with the test driver; the latter can be decompressed using tar xpvf data_medium.tar.xz. We also provide unoffical untested stub implementations in Go, Java and Rust.

A basic description of the workload is described in the following section. After the examples there is a detailed description of the semantics of each request type.

During the contest we will also hand out larger test datasets, similar to those used during evaluation.

####Running:
1. default input:
  "./execute"
2. path-to-input:
  "./execute ./path-to-input/file.bin [--tid]"
  (e.g. "./execute ./inputs/small.bin [--tid]")

[--tid is optional, enables hash to every transacrion-id]
