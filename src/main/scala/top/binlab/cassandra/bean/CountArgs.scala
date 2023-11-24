package top.binlab.cassandra.bean

import com.datastax.driver.core.{ConsistencyLevel, SocketOptions}
import org.apache.commons.cli.CommandLine

import scala.beans.BeanProperty

case class CountArgs(hosts: String, port: Int) {
  @BeanProperty
  var userName: String = _
  @BeanProperty
  var password: String = _
  @BeanProperty
  var ssl: Boolean = _
  @BeanProperty
  var keySpace: String = _
  @BeanProperty
  var table: String = _
  @BeanProperty
  var splitSize: Int = _
  @BeanProperty
  var threadSize: Int = _
  @BeanProperty
  var consistencyLevel: ConsistencyLevel = _
  @BeanProperty
  var connectTimeoutMillis: Int = _
  @BeanProperty
  var readTimeoutMillis: Int = _


  override def toString: String = {
    val sb = new StringBuilder()
    sb.append("hosts:").append(hosts)
    sb.append(",port:").append(port)
    sb.append(",userName:").append(userName)
    sb.append(",password:").append(password)
    sb.append(",ssl:").append(ssl)
    sb.append(",keySpace:").append(keySpace)
    sb.append(",table:").append(table)
    sb.append(",splitSize:").append(splitSize)
    sb.append(",threadSize:").append(threadSize)
    sb.append(",consistencyLevel:").append(consistencyLevel)
    sb.append(",connectTimeoutMillis:").append(connectTimeoutMillis)
    sb.append(",readTimeoutMillis:").append(readTimeoutMillis)
    sb.toString()
  }
}

object CountArgs {
  def apply(cmd: CommandLine): CountArgs = {
    val hosts = cmd.getOptionValue("hosts", "127.0.0.1")
    val port = cmd.getOptionValue("port", "9042").toInt
    val userName = cmd.getOptionValue("username", "")
    val password = cmd.getOptionValue("password", "")
    val ssl = cmd.hasOption("ssl")
    val keySpace = cmd.getOptionValue("keyspace")
    val table = cmd.getOptionValue("table")

    val splitSize = cmd.getOptionValue("s", "10").toInt
    val threadSize = cmd.getOptionValue("t", "1").toInt

    val cl = cmd.getOptionValue("consistancylevel")
    val consistencyLevel = if (cl != null && !cl.isEmpty()) {
      ConsistencyLevel.valueOf(cl)
    } else {
      ConsistencyLevel.LOCAL_QUORUM
    }
    val connectTimeout = cmd.getOptionValue("connecttimeout", SocketOptions.DEFAULT_CONNECT_TIMEOUT_MILLIS.toString).toInt
    val readTimeout = cmd.getOptionValue("readtimeout", SocketOptions.DEFAULT_READ_TIMEOUT_MILLIS.toString).toInt
    val withDF = cmd.hasOption("df")

    val args = new CountArgs(hosts, port)
    args.setUserName(userName)
    args.setPassword(password)
    args.setSsl(ssl)
    args.setKeySpace(keySpace)
    args.setTable(table)
    args.setSplitSize(splitSize)
    args.setThreadSize(threadSize)
    args.setConsistencyLevel(consistencyLevel)
    args.setConnectTimeoutMillis(connectTimeout)
    args.setReadTimeoutMillis(readTimeout)
    args
  }
}
