package top.binlab.cassandra.handler

import com.datastax.driver.core._
import org.slf4j.LoggerFactory
import top.binlab.cassandra.bean.TokenRange

import java.math.{BigInteger, RoundingMode}
import scala.beans.BeanProperty
import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer

class CassandraHandler(hosts: String, port: Int, userName: String, password: String, ssl: Boolean) {
  private val LOGGER = LoggerFactory.getLogger(getClass)

  @BeanProperty
  var connectTimeoutMillis: Int = SocketOptions.DEFAULT_CONNECT_TIMEOUT_MILLIS
  @BeanProperty
  var readTimeoutMillis: Int = SocketOptions.DEFAULT_READ_TIMEOUT_MILLIS

  private var cluster: Cluster = _


  def this(hosts: String, port: Int) {
    this(hosts, port, "", "", false)
  }

  def getCluster: Cluster = cluster

  def initCluster: Unit = {
    if (cluster != null) {
      return
    }
    val hostArr = hosts.split(",")
    val builder = Cluster.builder.withPort(port)
      .addContactPoints(hostArr: _*)
    val socketOptions = new SocketOptions
    socketOptions.setConnectTimeoutMillis(connectTimeoutMillis)
    socketOptions.setReadTimeoutMillis(readTimeoutMillis)
    builder.withSocketOptions(socketOptions)
    if (userName != null && !userName.isEmpty && password != null && !password.isEmpty) {
      builder.withCredentials(userName, password)
      if (ssl) {
        builder.withSSL()
      }
    }
    cluster = builder.build
  }


  def getSession(keySpace: String): Session = {
    cluster.connect(keySpace)
  }


  @throws[Exception]
  def checkExists(keySpace: String, table: String): Unit = {
    val ks = cluster.getMetadata().getKeyspace(keySpace)
    if (ks == null) {
      throw new Exception(s"keySpace not found:${keySpace}")
    }
    val tableMetadata = ks.getTable(table)
    if (tableMetadata == null) {
      throw new Exception(s"table not found:${table}")
    }
  }

  def destroy: Unit = {
    if (cluster != null) {
      cluster.close()
    }
  }

  def getPartitionKeys(tableMetadata: TableMetadata): Seq[ColumnMetadata] = {
    tableMetadata.getPartitionKey.asScala.toSeq
  }

  def getTokenRangeSeq(adviceNumber: Int): Seq[TokenRange] = {
    if (adviceNumber <= 1) {
      return Seq.empty[TokenRange]
    }
    val ranges = new ArrayBuffer[TokenRange]()
    val partitioner = cluster.getMetadata().getPartitioner()
    if (partitioner.endsWith("RandomPartitioner")) {
      val minToken = java.math.BigDecimal.valueOf(-1)
      val maxToken = new java.math.BigDecimal(new BigInteger("2").pow(127))
      val step = maxToken.subtract(minToken).divide(java.math.BigDecimal.valueOf(adviceNumber), 2, RoundingMode.HALF_EVEN)
      for (i <- 0 until adviceNumber) {
        val l = minToken.add(step.multiply(java.math.BigDecimal.valueOf(i))).toBigInteger()
        var r = minToken.add(step.multiply(java.math.BigDecimal.valueOf(i + 1))).toBigInteger()
        if (i == adviceNumber - 1) {
          r = maxToken.toBigInteger();
        }
        ranges += TokenRange(l.toString(), r.toString())
      }
    } else if (partitioner.endsWith("Murmur3Partitioner")) {
      val minToken = java.math.BigDecimal.valueOf(Long.MinValue)
      val maxToken = java.math.BigDecimal.valueOf(Long.MaxValue)
      val step = maxToken.subtract(minToken).divide(java.math.BigDecimal.valueOf(adviceNumber), 2, RoundingMode.HALF_EVEN)
      for (i <- 0 until adviceNumber) {
        val l = minToken.add(step.multiply(java.math.BigDecimal.valueOf(i))).longValue()
        var r = minToken.add(step.multiply(java.math.BigDecimal.valueOf(i + 1))).longValue()
        if (i == adviceNumber - 1) {
          r = maxToken.longValue();
        }
        ranges += TokenRange(l.toString, r.toString)
      }
    } else {
      return Seq.empty[TokenRange]
    }
    ranges.toSeq
  }


  def getQueryCountCql(table: String, tokenRange: TokenRange, partitionKeys: Seq[ColumnMetadata]): String = {
    val cqlSb = new StringBuilder()
    cqlSb.append("SELECT ")
    val pkSb = new StringBuilder()
     for (pk <- partitionKeys) {
      if (pkSb.length() > 0) {
        pkSb.append(",")
      }
      pkSb.append(pk.getName())
    }
    cqlSb.append(pkSb)
    //cqlSb.append("COUNT(1) as total") // 使用count(1)容易导致超时异常
    cqlSb.append(" FROM ")
    cqlSb.append(table)

    if (tokenRange == null) {
      val cql = cqlSb.toString()
      return cql
    }

    val s = pkSb.toString()
    val minToken = tokenRange.min
    val maxToken = tokenRange.max
    cqlSb.append(" WHERE token(").append(s).append(")").append(" > ").append(minToken)
    cqlSb.append(" AND token(").append(s).append(")").append(" <= ").append(maxToken)
    cqlSb.toString()
  }

  def execute(session: Session, cql: String, consistencyLevel: ConsistencyLevel): ResultSet = {
    LOGGER.info(s"execute cql:${cql}")
    session.execute(new SimpleStatement(cql).setConsistencyLevel(consistencyLevel))
  }
}
