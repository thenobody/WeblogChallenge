package net.antonvanco.paytm.weblogchallenge.transformation

import net.antonvanco.paytm.weblogchallenge.domain._
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.slf4j.LoggerFactory

/**
 * Created by antonvanco on 07/02/2016.
 */
class SessioniseLogEntriesTask(sessionLengthMillis: Long) {

  def execute(inputPath: String, sparkContext: SparkContext): RDD[((String, Session), Iterable[LogEntry])] = {
    sparkContext
      .textFile(inputPath)
      .repartition(16)
      .parseRawLogsToLogEntries
      .resolveSessionIdsForLogEntries(sessionLengthMillis)
      .aggregateLogEntriesByUserSession
  }
}

object SessioniseLogEntriesTask {
  val logger = LoggerFactory.getLogger(getClass)
}
