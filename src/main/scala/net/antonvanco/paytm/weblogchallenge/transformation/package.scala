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

  /**
   * Implicit transformations for raw ELB log lines
   */
  implicit class LogEntriesRDD(self: RDD[String]) {
    import LogEntriesTransformations._

    /**
     * Expects self to be an RDD of strings (lines) all of which are ELB log format
     * http://docs.aws.amazon.com/ElasticLoadBalancing/latest/DeveloperGuide/access-log-collection.html#access-log-entry-format
     *
     * @return RDD of parsed LogEntries indexed by their respective UserKey
     */
    def parseRawLogsToLogEntries = self.flatMap { line: String =>
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

  /**
   * Implicit RDD transformations for sessionising indexed LogEntries
   */
  implicit class PairedLogEntriesRDD[K, V](self: RDD[(K, V)]) {
    /**
     * Transformation groups LogEntries by their UserKey and proceeds to resolve session IDs on lists
     * of users' LogEntry items ordered by timestamp.
     *
     * Note: in its current form, this is probably the wobbliest part of the entire application since it aggregates
     * _all_ LogEntries of a given user in memory. Since we're grouping by UserKey, all user LogEntries will end up
     * in the same partition and the following flatMap processes these partitions individually. That means that
     * the processing can fall over if one user (or more) has more LogEntries than can fit into memory. This generally
     * might not be the case (LogEntry is only a couple of 100's of bytes, worker boxes usually have GBs of memory).
     * This can however become a problem in case when the input ELB log contains a very large time window.
     *
     * If such a problem become present the following code can be modified to subpartition LogEntries by time window,
     * i.e. sorting LogEntries by timestamp and grouping on a key composed of (UserKey, TimeWindow(timestamp)).
     * Afterwards, aggregateLogEntriesByUserSession will need to be modified to merge the session IDs into a single one
     * in case their start-end timestamps' difference does not exceed the sessionTimeout value.
     *
     * @param sessionTimeoutMillis
     *
     * @return RDD of LogEntry instances indexed by (UserKey, SessionID)
     */
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

    /**
     * Transforms an RDD of LogEntry instances indexed by (UserKey, SessionID) by merging LogEntries with the same
     * session ID under a single Session instance
     *
     * Note that in the current implementation a Session ends (maxTimestamp) is the timestamp of the last LogEntry
     * in the given session. This can be changed in case the session is defined as ending on maxTimestamp + sessionTimeoutMillis.
     *
     * @return RDD of (UserKey, Session) -> Iterable[LogEntry]
     */
    def aggregateLogEntriesByUserSession = self.asInstanceOf[RDD[((String, Int), LogEntry)]]
      .groupByKey()
      .map { case ((userId, sessionId), logEntries) =>
        val (minTimestamp, maxTimestamp) = logEntries.foldLeft(Long.MaxValue, Long.MinValue) { (acc, logEntry) =>
          val min = if (logEntry.timestamp < acc._1) logEntry.timestamp else acc._1
          val max = if (logEntry.timestamp > acc._2) logEntry.timestamp else acc._2
          (min, max)
        }
        val session = Session(sessionId, minTimestamp, maxTimestamp)
        (userId, session) -> logEntries
      }
  }

  /**
   * Implicit transformations for computing analytics metrics on sessionised LogEntries
   */
  implicit class UserSessionLogEntriesRDD(self: RDD[((String, Session), Iterable[LogEntry])]) {
    /**
     * Note that because the session is defined to end on the largest LogEntry timestamp in the session, a session length
     * can be 0 in case in contains only one LogEntry. To modify see note for PairedLogEntriesRDD.aggregateLogEntriesByUserSession
     */
    def getAverageSessionLength = {
      val (totalSessionLength, totalSessionCount) = self.aggregate((0L, 0L)) (
        { case ((lengthSum, totalCount), ((_, session), _)) => (lengthSum + session.duration, totalCount + 1) },
        { case (session1, session2) => (session1._1 + session2._1, session1._2 + session2._2) }
      )
      totalSessionLength.toDouble / totalSessionCount
    }

    def getUniqueUrisBySession = self.mapValues(_.map(_.requestUri).toSet)

    def getUserSessionsOrderedByUniqueUriCount = self.getUniqueUrisBySession
      .mapValues(_.size)
      .sortBy(_._2, ascending = false)

    /**
     * Note that a user may have multiple sessions with a long duration relative to other users and thus can be
     * present multiple times in the final output.
     * In order to get only the longest session length for unique users use getUsersOrderedBySessionLength
     *
     * @return RDD of UserKey -> session duration
     */
    def getUserSessionLengths = self.map { case ((userId, session), _) => userId -> session.duration }
      .sortBy(_._2, ascending = false)

    def getUsersOrderedBySessionLength = self.map { case ((userId, session), _) => userId -> session.duration }
      .reduceByKey(Math.max)
      .sortBy(_._2, ascending = false)

    def getSessionCountByUser = self.map { case ((userId, _), _ ) => userId -> 1L }
      .reduceByKey(_ + _)
      .sortBy(_._2, ascending = false)

    def getUserSessionCount = self.getSessionCountByUser.map(_._2).reduce(_ + _)

    def getUserCount = self.map { case ((userId, _), _ ) => userId }.distinct().count()

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
