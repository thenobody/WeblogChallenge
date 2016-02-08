package net.antonvanco.paytm.weblogchallenge.domain

import java.net.{URI, URISyntaxException}

import com.github.nscala_time.time.Imports._

import org.scalatest.{Matchers, FlatSpec}

/**
 * Created by antonvanco on 07/02/2016.
 */
class LogEntrySpec extends FlatSpec with Matchers {

  behavior of LogEntry.getClass.getSimpleName

  it should "instantiate a valid LogEntry" in {
    val expResult = new LogEntry(
      DateTime.parse("2016-01-01T01:00:00.000000Z").getMillis,
      "127.0.0.1",
      new URI("https://sub.localhost:8080/page/index.html?param1=value1&param2=value2"),
      "UserAgent"
    )
    val result = LogEntry("2016-01-01T01:00:00.000000Z", "127.0.0.1:1234", "GET https://sub.localhost:8080/page/index.html?param1=value1&param2=value2 HTTP/1.1", "UserAgent")

    result shouldEqual expResult
  }

  it should "reject invalid LogEntry values" in {
    intercept[URISyntaxException] { LogEntry("2016-01-01T01:00:00.000000Z", "127.0.0.1:1234", "GET {{ INVALID URI }} HTTP/1.1", "UserAgent") }
    intercept[IllegalArgumentException] { LogEntry("INVALID TIMESTAMP", "127.0.0.1:1234", "GET http://localhost HTTP/1.1", "UserAgent") }
    intercept[InvalidClientIpPortException] { LogEntry("2016-01-01T01:00:00.000000Z", "INVALID IP", "GET http://localhost HTTP/1.1", "UserAgent") }
    intercept[InvalidHttpRequestException] { LogEntry("2016-01-01T01:00:00.000000Z", "127.0.0.1:1234", "INVALID http://localhost HTTP/1.1", "UserAgent") }
    intercept[IllegalArgumentException] { LogEntry("2016-01-01T01:00:00.000000Z", "127.0.0.1:1234", "GET http://localhost HTTX/1.1", "UserAgent") }
    intercept[IllegalArgumentException] { LogEntry("2016-01-01T01:00:00.000000Z", "127.0.0.1:1234", "NOT A URL", "UserAgent") }
  }

}
