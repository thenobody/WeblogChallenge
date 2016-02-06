package net.antonvanco.paytm.weblogchallenge

import java.net.URI

import com.github.nscala_time.time.Imports._
import net.antonvanco.paytm.weblogchallenge.util.CsvUtil
import net.antonvanco.paytm.weblogchallenge.util.spark.{SparkConf, SparkContextLoan}
import org.slf4j.LoggerFactory

import scala.collection.immutable.ListMap
import scala.util.{Failure, Success, Try}

/**
 * Created by antonvanco on 06/02/2016.
 */
object Main extends SparkContextLoan {
  val logger = LoggerFactory.getLogger(getClass)
  val sessionTimeout = 15 * 60 * 1000 // 15 minutes

  def main(args: Array[String]): Unit = {
    val result = withSparkContextLoan(SparkConf(getClass.getSimpleName)) { sparkContext =>
      val userSessionsRdd = sparkContext
        .textFile("data/2015_07_22_mktplace_shop_web_log_sample.log.gz")
        .repartition(16)
        .flatMap { line =>
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
        }.groupByKey()
        .flatMap { case (userId, requests) =>
          val initSession = Session(0, Long.MinValue, Long.MinValue)

          requests.toList.sortBy(_.timestamp).foldLeft((initSession, Seq.empty[(Int, LogEntry)])) { (acc, logEntry) =>
            val (lastSession, sessionLogEntries) = acc
            val currentSession = if (logEntry.timestamp < lastSession.endTimestamp + sessionTimeout) {
              lastSession.copy(endTimestamp = Math.max(logEntry.timestamp, lastSession.endTimestamp))
            } else {
              Session(lastSession.id + 1, logEntry.timestamp, logEntry.timestamp)
            }

            (currentSession, sessionLogEntries :+ (currentSession.id, logEntry))
          }._2.map { case (sessionId, logEntry) => (userId, sessionId) -> logEntry }
        }.groupByKey()
        .map { case ((userId, sessionId), logEntries) =>
          val (minTimestamp, maxTimestamp) = logEntries.foldLeft(Long.MaxValue, Long.MinValue) { case (acc, logEntry) =>
            val min = if (logEntry.timestamp < acc._1) logEntry.timestamp else acc._1
            val max = if (logEntry.timestamp > acc._2) logEntry.timestamp else acc._2
            (min, max)
          }
          val session = Session(sessionId, minTimestamp, maxTimestamp)
          (userId, session) -> logEntries
        }.cache()

      userSessionsRdd.saveAsTextFile("data/parsed.log")

      // average session time
      val (totalSessionLength, totalSessionCount) = userSessionsRdd.aggregate((0L, 0L)) (
        { case ((lengthSum, totalCount), ((_, session), _)) => (lengthSum + session.duration, totalCount + 1) },
        { case (s1, s2) => (s1._1 + s2._1, s1._2 + s2._2) }
      )
      val avgSessionLength = totalSessionLength.toDouble / totalSessionCount
      logger.info(s"avgSessionLength: $avgSessionLength")

      // unique urls per session
      val uniqueSessionUris = userSessionsRdd.mapValues { logEntries => logEntries.map(_.request).groupBy(identity).keys }
      val topSessionUrisMap = ListMap(uniqueSessionUris.mapValues(_.size).sortBy(_._2, ascending = false).take(25): _*)
      logger.info(s"topSessionUris:\n${topSessionUrisMap.mkString("\n")}")

      // users with longest session times
      val longestUserSessions = userSessionsRdd.map { case ((userId, session), _) => userId -> session.duration }.sortBy(_._2, ascending = false).take(15)
      val longestUserSessionsMap = ListMap(longestUserSessions: _*)
      logger.info(s"longestUserSessions:\n${longestUserSessionsMap.mkString("\n")}")

      val userCount = userSessionsRdd.count()
      val userSessionsCounts = userSessionsRdd.mapValues(_.size).sortBy(_._2, ascending = false).take(10).toMap
      val avgUserSessionCount = userSessionsRdd.map(_._2.size).reduce(_ + _).toDouble / userCount
      logger.info(s"userCount: $userCount")
      logger.info(s"avgUserSessionCount: $avgUserSessionCount")
      logger.info(s"top session counts:\n${userSessionsCounts.mkString("\n")}")
    }
  }

  def reduceSessions(acc: (Long, Int), userSessions: (String, Seq[(Session, LogEntry)])): (Long, Int) = {
    val sd = userSessions._2.map { session => (session._1.duration, 1) }.reduce((s1, s2) => (s1._1 + s2._1, s1._2 + s2._2))
    (acc._1 + sd._1, acc._2 + sd._2)
  }

  def reduceSessionDurations(session1: (Long, Int), session2: (Long, Int)): (Long, Int) = {
    (session1._1 + session2._1, session1._2 + session2._2)
  }
}

object UserKey {
  def apply(logEntry: LogEntry): String = logEntry.clientIp
}

case class Session(id: Int, startTimestamp: Long, endTimestamp: Long) {
  def duration = endTimestamp - startTimestamp
}

case class LogEntry(timestamp: Long, clientIp: String, request: URI, userAgent: String)

object LogEntry {
  val validHttpMethods = Seq("GET", "POST", "HEAD", "OPTION", "PUT", "DELETE")
  val requestRegex = s"""(${validHttpMethods.mkString("|")}) (.*) HTTP/1.[0-9]""".r
  def apply(timestamp: String, clientIp: String, request: String, userAgent: String): LogEntry = {
    val timestampMillis = new DateTime(timestamp).getMillis
    val uri = request match {
      case requestRegex(_, uriString) => new URI(uriString)
      case invalid => throw new IllegalArgumentException(invalid)
    }

    LogEntry(timestampMillis, clientIp, uri, userAgent)
  }
}
