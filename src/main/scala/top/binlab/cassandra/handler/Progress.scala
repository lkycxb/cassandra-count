package top.binlab.cassandra.handler

import org.slf4j.LoggerFactory

import java.text.DecimalFormat

class Progress(val total: Long, step: Long = 1, sleepMills: Long = -1l) {
  private val logger = LoggerFactory.getLogger(getClass)
  private val df2 = new DecimalFormat("0.00")
  private var currentCount = 0L

  private var lastGetProgressCount = 0L

  private var isRunning = true

  def startThread(tag: String = "progress"): Unit = {
    if (sleepMills > 0) {
      val thread = new Thread(() => {
        while (isRunning) {
          if (hasOverStep) {
            logger.info(s"${tag} progress:${getProgressFullStr()}")
          }
          Thread.sleep(sleepMills)
        }
      })
      thread.start()
    }
  }

  def destroy(): Unit = isRunning = false

  def increment(count: Long = 1): Unit = synchronized {
    currentCount += count
  }

  def hasOverStep(): Boolean = {
    currentCount - lastGetProgressCount >= step
  }

  def getProgress(): Double = {
    if (total <= 0) {
      return 0.0d
    }
    lastGetProgressCount = currentCount
    currentCount.toDouble / total.toDouble
  }

  def getProgressStr(): String = {
    df2.format(getProgress() * 100) + "%"
  }

  def getProgressFullStr(): String = {
    new StringBuilder().append(currentCount).append("/").append(total)
      .append(",")
      .append(df2.format(getProgress() * 100)).append("%")
      .toString()
  }

  def getCurrentCount = currentCount

  override def toString: String = getProgressFullStr()
}
