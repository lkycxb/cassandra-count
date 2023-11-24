# cassandra-count
Count rows in Cassandra Table

Simple program to count the number of records in a Cassandra table.
By splitting the token range using the numSplits parameter, you can
reduce the amount each query is counting and reduce the probability
of timeouts.

It is true the Spark is well-suited to this operation, however the
goal of this program is to be a simple utility that does not require
Spark.  This is useful for simple debugging of loading data (and other
data quality tasks).When the data is large enough, such as 3billion,
the query timeout exception will occur when using spark+cassandra. 
In addition, using spark+cassandra to query is like SQL statement 
'select * from<table>', which wastes query performance. This tool 
uses 'select<partition key>from<table>' to optimize.

## Getting it
### Downloading
