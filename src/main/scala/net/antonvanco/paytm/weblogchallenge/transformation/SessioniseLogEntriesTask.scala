package net.antonvanco.paytm.weblogchallenge.transformation

import net.antonvanco.paytm.weblogchallenge.domain._
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.slf4j.LoggerFactory

/**
 * Created by antonvanco on 07/02/2016.
 *
 * The main transformation task for creating sessions from ELB log input
 *
 * @param sessionLengthMillis defines the max timestamp difference between user's log entries in order to belong
 *                            into the same session
 */
class SessioniseLogEntriesTask(sessionLengthMillis: Long, inputPartitionCount: Int) {

  def execute(inputPath: String, sparkContext: SparkContext): RDD[((String, Session), Iterable[LogEntry])] = {
    sparkContext
      .textFile(inputPath)
      // repartitioning because the expected input is Gzipped and by default Spark creates a single partition when reading compressed input
      .repartition(inputPartitionCount)
      .parseRawLogsToLogEntries
      .resolveSessionIdsForLogEntries(sessionLengthMillis)
      .aggregateLogEntriesByUserSession
  }
}

object SessioniseLogEntriesTask {
  val logger = LoggerFactory.getLogger(getClass)
}
