# cassandra-count
计算Cassandra数据加的记录行数


计算Cassandra表中记录数的简单程序。

通过使用splitSize参数分割令牌范围，您可以减少每个查询计数的数量以降低超时次数的概率.

Spark确实是适合Cassandra的各种操作,这个程序的不依赖Spark的一个轻量级的计数器,
和Spark相比,该程序在一些方面做了优化,比如Spark加载数据是记录的所有字段都读取,
对于只需要统计记录数的功能来说完全是浪费性能的,该程序只读取Partition Key对应的字段,
然后计数.如Spark读取相当于SQL:'select * from<table>',该程序读取时的SQL:
'select <partition key> from<table>'.

## 获得程序
### 直接下载
此实用程序已预编译，可在下面链接找到:
https://github.com/topbinlab/cassandra-count/releases/download/v0.0.1/cassandra-count-0.0.1.jar

使用wget下载:
```
wget https://github.com/topbinlab/cassandra-count/releases/download/v0.0.1/cassandra-count-0.0.1.jar
```

### 编译
从仓库克隆该代码:
```
git clone https://github.com/topbinlab/cassandra-count.git
```
编译:
```
cd cassandra-count
mvn clean package -DskipTests=true
```

### 运行 
Jar文件可通过如下命令运行:
```
java -jar cassandra-count-0.0.1.jar -hosts 127.0.0.1 -keyspace test -table student
```
也可以添加一些JVM参数,例如:
```
java -Xmx2G -jar cassandra-count-0.0.1.jar -keyspace test -table student
```

### Usage
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

### Examples
java -jar cassandra-count-0.0.1.jar -hosts 127.0.0.1 -keyspace test -table student -s 10
java -jar cassandra-count-0.0.1.jar -hosts 192.168.1.2,192.168.1.3 -p 9042 -keyspace test -table student -s 100

### Note
When the amount of data is too large, the splitsize parameter value can be increased
```
java -jar cassandra-count-0.0.1.jar -hosts 127.0.0.1 -keyspace test -table student -s 100000
```

