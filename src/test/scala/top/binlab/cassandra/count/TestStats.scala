package top.binlab.cassandra.count


import junit.framework.TestCase

import scala.collection.mutable.ArrayBuffer

/**
 *
 * 159single:
 * binary:464761
 * function:266588583
 * block:3416046972
 *
 * 40single:
 * binary: 464761
 * function: 266588583
 * block: 3416046972
 *
 * linux:
 * java -cp "lib/*" top.binlab.cassandra.count.CountJob -hosts 127.0.0.1 -keyspace test -table student -s 100 -t 5
 *
 * */
 */

class TestStats extends TestCase {

  def testCount40Single(): Unit = {
    val argsBuff = new ArrayBuffer[String]()

    argsBuff += "-hosts"
    argsBuff += "192.168.1.40"
    argsBuff += "-port"
    argsBuff += "9042"
    argsBuff += "-connecttimeout"
    argsBuff += "6000"
    argsBuff += "-readtimeout"
    argsBuff += "6000"


    argsBuff += "-keyspace"
    argsBuff += "dag"
    argsBuff += "-table"
    argsBuff += "global_binary"
    //    argsBuff += "global_function"
    //    argsBuff += "global_block"

    argsBuff += "-t"
    argsBuff += "3"
    argsBuff += "-s"
    argsBuff += "1000"
    argsBuff += "-consistancylevel"
    argsBuff += "LOCAL_ONE"
    CountJob.main(argsBuff.toArray)
  }

  def testCount159Single(): Unit = {
    val argsBuff = new ArrayBuffer[String]()

    argsBuff += "-hosts"
    argsBuff += "192.168.1.159"
    argsBuff += "-port"
    argsBuff += "9042"
    argsBuff += "-connecttimeout"
    argsBuff += "600000"
    argsBuff += "-readtimeout"
    argsBuff += "600000"


    argsBuff += "-keyspace"
    argsBuff += "dag"
    argsBuff += "-table"
    //    argsBuff += "global_binary"
    argsBuff += "global_block"

    argsBuff += "-t"
    argsBuff += "2"
    argsBuff += "-s"
    argsBuff += "100000"
    argsBuff += "-consistancylevel"
    argsBuff += "LOCAL_ONE"
    CountJob.main(argsBuff.toArray)
  }

  def testCount159225Cluster(): Unit = {
    val argsBuff = new ArrayBuffer[String]()

    argsBuff += "-hosts"
    argsBuff += "192.168.1.159,192.168.1.225"
    argsBuff += "-port"
    argsBuff += "9042"
    argsBuff += "-connecttimeout"
    argsBuff += "600000"
    argsBuff += "-readtimeout"
    argsBuff += "600000"


    argsBuff += "-keyspace"
    argsBuff += "dag"
    argsBuff += "-table"
    //    argsBuff += "global_binary"
    argsBuff += "global_block"

    argsBuff += "-t"
    argsBuff += "1"
    argsBuff += "-s"
    argsBuff += "300"
    argsBuff += "-consistancylevel"
    argsBuff += "LOCAL_ONE"
    CountJob.main(argsBuff.toArray)
  }

  def testCount241SingleTemp(): Unit = {
    val argsBuff = new ArrayBuffer[String]()

    argsBuff += "-hosts"
    argsBuff += "192.88.1.241"
    argsBuff += "-port"
    argsBuff += "9042"
    argsBuff += "-connecttimeout"
    argsBuff += "600000"
    argsBuff += "-readtimeout"
    argsBuff += "600000"


    argsBuff += "-keyspace"
    argsBuff += "dag"
    argsBuff += "-table"
    //    argsBuff += "global_binary"
    argsBuff += "global_block"

    argsBuff += "-t"
    argsBuff += "2"
    argsBuff += "-s"
    argsBuff += "100"
    argsBuff += "-consistancylevel"
    argsBuff += "LOCAL_ONE"
    CountJob.main(argsBuff.toArray)
  }
}
