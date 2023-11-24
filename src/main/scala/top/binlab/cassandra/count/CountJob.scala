package top.binlab.cassandra.count

import com.datastax.driver.core._
import org.apache.commons.cli.CommandLine
import org.slf4j.LoggerFactory
import top.binlab.cassandra.bean.{CountArgs, TokenRange}
import top.binlab.cassandra.handler.{CassandraHandler, Progress, TraitCommandLineHandler}

import java.util.concurrent.ForkJoinPool
import scala.collection.parallel.ForkJoinTaskSupport
import scala.jdk.CollectionConverters._

class CountJob(cmd: CommandLine) {
  private val LOGGER = LoggerFactory.getLogger(getClass)
  private var args: CountArgs = _

  private var cassandraHandler: CassandraHandler = _
  private var session: Session = _


  private def init: Unit = {
    args = CountArgs(cmd)
    cassandraHandler = new CassandraHandler(args.hosts, args.port, args.getUserName, args.getPassword, args.getSsl)
    cassandraHandler.setConnectTimeoutMillis(args.getConnectTimeoutMillis)
    cassandraHandler.setReadTimeoutMillis(args.getReadTimeoutMillis)
    cassandraHandler.initCluster
    cassandraHandler.checkExists(args.getKeySpace, args.getTable)
    session = cassandraHandler.getSession(args.getKeySpace)
    LOGGER.info(s"args:${args},")
  }

  private def destroy: Unit = {
    cassandraHandler.destroy
  }

  private def getCountByRange(table: String, tokenRange: TokenRange, partitionKeys: Seq[ColumnMetadata])(implicit progress: Progress): Long = {
    val cql = cassandraHandler.getQueryCountCql(table, tokenRange, partitionKeys)
    val startTime = System.currentTimeMillis()
    if (LOGGER.isDebugEnabled()) {
      LOGGER.debug(s"execute start cql:${cql}")
    }
    val rs = session.execute(new SimpleStatement(cql).setConsistencyLevel(args.getConsistencyLevel)).asScala
    if (progress != null) {
      progress.increment()
    }
    val rowCount = processRowIterable(rs)
    if (LOGGER.isDebugEnabled()) {
      val elapsed = System.currentTimeMillis() - startTime
      LOGGER.debug(s"execute done,elapsed:${elapsed},rowCount:${rowCount},cql:${cql}")
    }
    rowCount
  }

  private def processRowIterable(rsIterable: Iterable[Row]): Long = {
    var total = 0L
    for (_ <- rsIterable) total += 1
    total
  }

  private def countByTokenRanges(cluster: Cluster, table: String, tokenRanges: Seq[TokenRange]): Long = {
    implicit val progress = new Progress(tokenRanges.size, 10L, 60000L)
    progress.startThread("countByTokenRanges")
    val ks = cluster.getMetadata().getKeyspace(args.getKeySpace)
    val tableMetadata = ks.getTable(table)
    val partitionKeys = cassandraHandler.getPartitionKeys(tableMetadata)

    var totalCount = 0L
    if (tokenRanges.isEmpty) {
      totalCount = getCountByRange(table, null, partitionKeys)
      progress.destroy()
      return totalCount
    }

    val threadSize = Math.min(args.getThreadSize, tokenRanges.size)
    var taskSupport: ForkJoinTaskSupport = null
    if (threadSize > 1 && tokenRanges.size > 1) {
      import scala.collection.parallel.CollectionConverters._
      val parTokenRanges = tokenRanges.par
      taskSupport = new ForkJoinTaskSupport(new ForkJoinPool(threadSize))
      parTokenRanges.tasksupport = taskSupport
      totalCount += parTokenRanges.map(getCountByRange(table, _, partitionKeys)).sum
      if (taskSupport != null) {
        taskSupport.environment.shutdown()
      }
    } else {
      totalCount += tokenRanges.map(getCountByRange(table, _, partitionKeys)).sum
    }
    progress.destroy()
    totalCount
  }

  private def countByTable(cluster: Cluster, table: String): Unit = {
    val tokenRanges = cassandraHandler.getTokenRangeSeq(args.getSplitSize)
    LOGGER.info(s"tokenRanges size:${tokenRanges.size}")
    val total = countByTokenRanges(cluster, table, tokenRanges)
    val msg = s"keySpace:${args.getKeySpace},table:${table},rowCount:${total}"
    LOGGER.info(msg)
    println(msg)
  }

  private def process: Unit = {
    val cluster = cassandraHandler.getCluster
    try {
      countByTable(cluster, args.getTable)
    } catch {
      case e: Throwable => LOGGER.error("process error", e)
    }
  }

  def run(): Unit = {
    init
    process
    destroy
  }
}


object CountJob extends TraitCommandLineHandler {
  def main(args: Array[String]): Unit = {
    initOptions("CountJob")
    addRequireOption("hosts", true, "hosts,default:127.0.0.1")
    addOption("port", true, "port,default:9042")
    addOption("username", true, "username")
    addOption("password", true, "password")
    addOption("ssl", false, "use SSL")

    addRequireOption("keyspace", true, "keySpace")
    addRequireOption("table", true, "table")
    addOption("s", true, "splitSize,default:10")
    addOption("t", true, "threadSize,default:1")
    addOption("connecttimeout", true, "connectTimeoutMillis,default:5000")
    addOption("readtimeout", true, "readTimeoutMillis,default:12000")
    addOption("consistancylevel", true, "consistancyLevel,default:LOCAL_QUORUM")

    val cmd = parseCommandLine(args)
    if (cmd == null) {
      return
    }
    val handler = new CountJob(cmd)
    handler.run
  }
}