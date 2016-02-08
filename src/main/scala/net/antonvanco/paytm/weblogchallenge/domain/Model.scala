package net.antonvanco.paytm.weblogchallenge.domain

import java.net.URI

import com.github.nscala_time.time.Imports._

/**
 * Created by antonvanco on 07/02/2016.
 */
object UserKey {
  /**
   * As noted in the assignment IPs are generally not a good estimator of a single user.
   * Additional layer of 'uniqueness' could be added by creating the UserKey from user's IP address _and_ UserAgent string.
   * The assumption being that multiple users under the same IP (such as a household with multiple devices) will be using
   * different browsers.
   * Another approach would be to use Cookie-based session IDs which would be set, expired and renewed by our web service
   * serving individual users' browsers. In fact, if this were the case it would make PairedLogEntriesRDD.resolveSessionIdsForLogEntries
   * would become obsolete and could be skipped.
   *
   * @param logEntry
   * @return String UserKey
   */
  def apply(logEntry: LogEntry): String = logEntry.clientIp
}

case class Session(id: Int, startTimestamp: Long, endTimestamp: Long) {
  def duration = endTimestamp - startTimestamp
}

case class LogEntry(timestamp: Long, clientIp: String, requestUri: URI, userAgent: String)

object LogEntry {
  val ValidHttpMethods = Seq("GET", "POST", "HEAD", "OPTION", "PUT", "DELETE")
  val RequestRegex = s"""(${ValidHttpMethods.mkString("|")}) (.*) HTTP/1.[0-9]""".r
  val ipPortRegex = """(([0-9]{1,3}\.){3}[0-9]{1,3})\:([0-9]{1,5})""".r

  def apply(timestamp: String, clientIpPort: String, request: String, userAgent: String): LogEntry = {
    val timestampMillis = new DateTime(timestamp).getMillis
    val uri = request match {
      case RequestRegex(_, uriString) => new URI(uriString)
      case invalid => throw new InvalidHttpRequestException(invalid)
    }
    val clientIp = clientIpPort match {
      case ipPortRegex(ip, _, port) => ip
      case invalid => throw new InvalidClientIpPortException(invalid)
    }

    LogEntry(timestampMillis, clientIp, uri, userAgent)
  }
}

case class InvalidHttpRequestException(request: String) extends IllegalArgumentException(request)
case class InvalidClientIpPortException(clientIpPort: String) extends IllegalArgumentException(clientIpPort)
