package net.antonvanco.paytm.weblogchallenge

import net.antonvanco.paytm.weblogchallenge.util.CsvUtil
import net.antonvanco.paytm.weblogchallenge.domain._
import org.apache.spark.rdd.RDD
import org.slf4j.LoggerFactory

import scala.util.{Failure, Success, Try}

/**
 * Created by antonvanco on 07/02/2016.
 */
package object transformation {
  implicit class LogEntriesRDD[T](self: RDD[T]) {
    import LogEntriesTransformations._
    
    def parseRawLogsToLogEntries = self.flatMap { case line: String =>
      Try {
        CsvUtil.parseCsv(line, ' ') match {
          case List(timestamp, _, clientIp, _, _, _, _, _, _, _, _, request, userAgent, _, _) =>
            LogEntry(timestamp, clientIp, request, userAgent)
        }
      } match {
        case Success(logEntry) => Some(UserKey(logEntry) -> logEntry)
        case Failure(exception) =>
          logger.warn(s"Invalid log entry: $line", exception)
          None
      }
    }
  }

  implicit class PairedLogEntriesRDD[K, V](self: RDD[(K, V)]) {
    def resolveSessionIdsForLogEntries(sessionTimeoutMillis: Long) = self.asInstanceOf[RDD[(String, LogEntry)]]
      .groupByKey()
      .flatMap { case (userId, requests) =>
        val initSession = Session(0, Long.MinValue, Long.MinValue)

        requests.toList.sortBy(_.timestamp).foldLeft((initSession, Seq.empty[(Int, LogEntry)])) { (acc, logEntry) =>
          val (lastSession, sessionLogEntries) = acc
          val currentSession = if (logEntry.timestamp < lastSession.endTimestamp + sessionTimeoutMillis) {
            lastSession.copy(endTimestamp = Math.max(logEntry.timestamp, lastSession.endTimestamp))
          } else {
            Session(lastSession.id + 1, logEntry.timestamp, logEntry.timestamp)
          }

          (currentSession, sessionLogEntries :+ (currentSession.id, logEntry))
        }._2.map { case (sessionId, logEntry) => (userId, sessionId) -> logEntry }
      }

    def aggregateLogEntriesByUserSession = self.asInstanceOf[RDD[((String, Int), LogEntry)]]
      .groupByKey()
      .map { case ((userId, sessionId), logEntries) =>
        val (minTimestamp, maxTimestamp) = logEntries.foldLeft(Long.MaxValue, Long.MinValue) { case (acc, logEntry) =>
          val min = if (logEntry.timestamp < acc._1) logEntry.timestamp else acc._1
          val max = if (logEntry.timestamp > acc._2) logEntry.timestamp else acc._2
          (min, max)
        }
        val session = Session(sessionId, minTimestamp, maxTimestamp)
        (userId, session) -> logEntries
      }
  }

  implicit class UserSessionLogEntriesRDD(self: RDD[((String, Session), Iterable[LogEntry])]) {
    def getAverageSessionLength = {
      val (totalSessionLength, totalSessionCount) = self.aggregate((0L, 0L)) (
        { case ((lengthSum, totalCount), ((_, session), _)) => (lengthSum + session.duration, totalCount + 1) },
        { case (session1, session2) => (session1._1 + session2._1, session1._2 + session2._2) }
      )
      totalSessionLength.toDouble / totalSessionCount
    }

    def getUniqueUrisBySession = self.mapValues(_.map(_.request).groupBy(identity).keys)

    def getUserSessionsOrderedByUniqueUriCount = self.getUniqueUrisBySession
      .mapValues(_.size)
      .sortBy(_._2, ascending = false)

    def getUsersOrderedBySessionLength = self.map { case ((userId, session), _) => userId -> session.duration }
      .sortBy(_._2, ascending = false)

    def getSessionCountByUser = self.map { case ((userId, _), _ ) => userId -> 1L }
      .reduceByKey(_ + _)
      .sortBy(_._2, ascending = false)

    def getUserSessionCount = self.getSessionCountByUser.map(_._2).reduce(_ + _)

    def getUserCount = self.map { case ((userId, _), _ ) => userId -> null }.keys.count()

    def getAverageSessionCount = {
      val (totalUserCount, totalSessionCount) = self.getSessionCountByUser
        .map { case (_, sessionCount) => (1L, sessionCount) }
        .reduce { case (userSessionCount1, userSessionCount2) =>
          (userSessionCount1._1 + userSessionCount2._1, userSessionCount1._2 + userSessionCount2._2)
        }
      totalSessionCount.toDouble / totalUserCount
    }
  }

  object LogEntriesTransformations {
    val logger = LoggerFactory.getLogger(getClass)
  }
}
