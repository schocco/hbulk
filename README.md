Hbase Bulk Load Example
========================

Simple Hbase Bulk Load example to load the enron email corpus into a table.

Setup
------

- download and unpack the enron emails from [cs.cmu.edu/~./enron/](https://www.cs.cmu.edu/~./enron/)
- reference the `conf` folder of your hbase installation when running a standalone setup or manually set up the configuration in the `BulkLoad` class
- get the maven dependencies
- run the program with the arguments `unpacked emails` `target path for hfile` `table name for bulk import`

Note that the mapreduce job might consume a lot of memory so that excessive GC is needed.  
You might want to split the task into smaller portions when running on a single node.
