package net.antonvanco.paytm.weblogchallenge.domain

import java.net.URI

import com.github.nscala_time.time.Imports._

/**
 * Created by antonvanco on 07/02/2016.
 */
object UserKey {
  def apply(logEntry: LogEntry): String = logEntry.clientIp
}

case class Session(id: Int, startTimestamp: Long, endTimestamp: Long) {
  def duration = endTimestamp - startTimestamp
}

case class LogEntry(timestamp: Long, clientIp: String, request: URI, userAgent: String)

object LogEntry {
  val ValidHttpMethods = Seq("GET", "POST", "HEAD", "OPTION", "PUT", "DELETE")
  val RequestRegex = s"""(${ValidHttpMethods.mkString("|")}) (.*) HTTP/1.[0-9]""".r
  
  def apply(timestamp: String, clientIp: String, request: String, userAgent: String): LogEntry = {
    val timestampMillis = new DateTime(timestamp).getMillis
    val uri = request match {
      case RequestRegex(_, uriString) => new URI(uriString)
      case invalid => throw new IllegalArgumentException(invalid)
    }

    LogEntry(timestampMillis, clientIp, uri, userAgent)
  }
}
