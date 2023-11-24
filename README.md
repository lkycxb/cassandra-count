# cassandra-count
Count rows in Cassandra Table

Simple program to count the number of records in a Cassandra table.
By splitting the token range using the splitSize parameter, you can
reduce the amount each query is counting and reduce the probability
of timeouts.

It is true the Spark is well-suited to this operation, however the
goal of this program is to be a simple utility that does not require
Spark.  This is useful for simple debugging of loading data (and other
data quality tasks).When the data is large enough, such as 3billion,
the query timeout exception will occur when using spark+cassandra. 
In addition, using spark+cassandra to query is like SQL statement 
'select * from [table]', which wastes query performance. This tool 
uses 'select [partition key] from [table]' to optimize.

## Getting it
### Downloading
This utility has already been built, and is available at
https://github.com/topbinlab/cassandra-count/releases/download/v0.0.1/cassandra-count-0.0.1.jar

Get it with wget:
```
wget https://github.com/topbinlab/cassandra-count/releases/download/v0.0.1/cassandra-count-0.0.1.jar
```

### Building
Clone code:
```
git clone https://github.com/topbinlab/cassandra-count.git
```

To build this repository, simply clone this repo and run:
```
mvn clean package -DskipTests=true
```

## Run 
The jar file so you can also run via
```
java -jar cassandra-count-0.0.1.jar -hosts 127.0.0.1 -keyspace test -table student
```
And add whatever extra arguments you want. For example:
```
java -Xmx2G -jar cassandra-count-0.0.1.jar -keyspace test -table student
```


## Usage
```
java -jar cassandra-count-0.0.1.jar
usage: CountJob
-connecttimeout <arg>     connectTimeoutMillis,default:5000
-consistancylevel <arg>   consistancyLevel,default:LOCAL_QUORUM
-hosts <arg>              hosts,default:127.0.0.1
-keyspace <arg>           keySpace
-password <arg>           password
-port <arg>               port,default:9042
-readtimeout <arg>        readTimeoutMillis,default:12000
-s <arg>                  splitSize,default:10
-ssl                      use SSL
-t <arg>                  threadSize,default:1
-table <arg>              table
-username <arg>           username
```

## Examples
java -jar cassandra-count-0.0.1.jar -hosts 127.0.0.1 -keyspace test -table student -s 10
java -jar cassandra-count-0.0.1.jar -hosts 192.168.1.2,192.168.1.3 -p 9042 -keyspace test -table student -s 100

## Note
When the amount of data is too large, the splitsize parameter value can be increased
```
java -jar cassandra-count-0.0.1.jar -hosts 127.0.0.1 -keyspace test -table student -s 100000
```
## Contact
微信:lkycxb
Mail:xbings@163.com

