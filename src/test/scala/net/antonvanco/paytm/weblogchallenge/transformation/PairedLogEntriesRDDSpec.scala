package net.antonvanco.paytm.weblogchallenge.transformation

import java.net.URI

import net.antonvanco.paytm.weblogchallenge.domain.{Session, LogEntry}
import net.antonvanco.paytm.weblogchallenge.util.spark.{SparkConf, SparkContextLoan}
import org.scalatest.{Matchers, FlatSpec}

/**
 * Created by antonvanco on 07/02/2016.
 */
class PairedLogEntriesRDDSpec extends FlatSpec with Matchers with SparkContextLoan {

  behavior of classOf[PairedLogEntriesRDD[_, _]].getSimpleName

  val sparkConf = SparkConf(getClass.getSimpleName, "local[*]")

  it should "resolve session IDs for LogEntries" in withSparkContext(sparkConf) { sparkContext =>
    val currentMillis = System.currentTimeMillis
    val sessionTimeoutMillis = 5000L
    val inputRdd = sparkContext.parallelize(Seq(
      "127.0.0.1" -> new LogEntry(currentMillis, "127.0.0.1", new URI("https://localhost:1234/page1"), "UserAgent"),
      "127.0.0.1" -> new LogEntry(currentMillis + 1000, "127.0.0.1", new URI("https://localhost:1234/page2"), "UserAgent"),
      "127.0.0.1" -> new LogEntry(currentMillis + sessionTimeoutMillis + 1000, "127.0.0.1", new URI("https://localhost:1234/page3"), "UserAgent"),
      "127.0.0.1" -> new LogEntry(currentMillis + sessionTimeoutMillis + 2000, "127.0.0.1", new URI("https://localhost:1234/page4"), "UserAgent"),

      "127.0.0.2" -> new LogEntry(currentMillis, "127.0.0.2", new URI("https://localhost:1234/page1"), "UserAgent"),
      "127.0.0.2" -> new LogEntry(currentMillis + sessionTimeoutMillis + 1000, "127.0.0.2", new URI("https://localhost:1234/page3"), "UserAgent"),
      "127.0.0.2" -> new LogEntry(currentMillis + 2 * sessionTimeoutMillis + 1000, "127.0.0.2", new URI("https://localhost:1234/page4"), "UserAgent")
    ))

    val expResult = Seq(
      ("127.0.0.1", 1) -> new LogEntry(currentMillis, "127.0.0.1", new URI("https://localhost:1234/page1"), "UserAgent"),
      ("127.0.0.1", 1) -> new LogEntry(currentMillis + 1000, "127.0.0.1", new URI("https://localhost:1234/page2"), "UserAgent"),
      ("127.0.0.1", 2) -> new LogEntry(currentMillis + sessionTimeoutMillis + 1000, "127.0.0.1", new URI("https://localhost:1234/page3"), "UserAgent"),
      ("127.0.0.1", 2) -> new LogEntry(currentMillis + sessionTimeoutMillis + 2000, "127.0.0.1", new URI("https://localhost:1234/page4"), "UserAgent"),

      ("127.0.0.2", 1) -> new LogEntry(currentMillis, "127.0.0.2", new URI("https://localhost:1234/page1"), "UserAgent"),
      ("127.0.0.2", 2) -> new LogEntry(currentMillis + sessionTimeoutMillis + 1000, "127.0.0.2", new URI("https://localhost:1234/page3"), "UserAgent"),
      ("127.0.0.2", 3) -> new LogEntry(currentMillis + 2 * sessionTimeoutMillis + 1000, "127.0.0.2", new URI("https://localhost:1234/page4"), "UserAgent")
    )

    val result = inputRdd.resolveSessionIdsForLogEntries(sessionTimeoutMillis).collect().toSeq

    result should contain theSameElementsAs expResult
  }

  it should "aggregateLogEntriesByUserSession" in withSparkContext(sparkConf) { sparkContext =>
    val currentMillis = System.currentTimeMillis
    val sessionTimeoutMillis = 5000L

    val inputRdd = sparkContext.parallelize(Seq(
      ("127.0.0.1", 1) -> new LogEntry(currentMillis, "127.0.0.1", new URI("https://localhost:1234/page1"), "UserAgent"),
      ("127.0.0.1", 1) -> new LogEntry(currentMillis + 1000, "127.0.0.1", new URI("https://localhost:1234/page2"), "UserAgent"),
      ("127.0.0.1", 2) -> new LogEntry(currentMillis + sessionTimeoutMillis + 1000, "127.0.0.1", new URI("https://localhost:1234/page3"), "UserAgent"),
      ("127.0.0.1", 2) -> new LogEntry(currentMillis + sessionTimeoutMillis + 2000, "127.0.0.1", new URI("https://localhost:1234/page4"), "UserAgent"),

      ("127.0.0.2", 1) -> new LogEntry(currentMillis, "127.0.0.2", new URI("https://localhost:1234/page1"), "UserAgent"),
      ("127.0.0.2", 2) -> new LogEntry(currentMillis + sessionTimeoutMillis + 1000, "127.0.0.2", new URI("https://localhost:1234/page3"), "UserAgent"),
      ("127.0.0.2", 3) -> new LogEntry(currentMillis + 2 * sessionTimeoutMillis + 1000, "127.0.0.2", new URI("https://localhost:1234/page4"), "UserAgent")
    ))

    val userSession1 = Session(1, currentMillis, currentMillis + 1000)
    val userSession2 = Session(2, currentMillis + sessionTimeoutMillis + 1000, currentMillis + sessionTimeoutMillis + 2000)

    val userSession3 = Session(1, currentMillis, currentMillis)
    val userSession4 = Session(2, currentMillis + sessionTimeoutMillis + 1000, currentMillis + sessionTimeoutMillis + 1000)
    val userSession5 = Session(3, currentMillis + 2 * sessionTimeoutMillis + 1000, currentMillis + 2 * sessionTimeoutMillis + 1000)

    val expResult = Seq(
      ("127.0.0.1", userSession1) -> Seq(
        new LogEntry(currentMillis, "127.0.0.1", new URI("https://localhost:1234/page1"), "UserAgent"),
        new LogEntry(currentMillis + 1000, "127.0.0.1", new URI("https://localhost:1234/page2"), "UserAgent")
      ),
      ("127.0.0.1", userSession2) -> Seq(
        new LogEntry(currentMillis + sessionTimeoutMillis + 1000, "127.0.0.1", new URI("https://localhost:1234/page3"), "UserAgent"),
        new LogEntry(currentMillis + sessionTimeoutMillis + 2000, "127.0.0.1", new URI("https://localhost:1234/page4"), "UserAgent")
      ),

      ("127.0.0.2", userSession3) -> Seq(
        new LogEntry(currentMillis, "127.0.0.2", new URI("https://localhost:1234/page1"), "UserAgent")
      ),
      ("127.0.0.2", userSession4) -> Seq(
        new LogEntry(currentMillis + sessionTimeoutMillis + 1000, "127.0.0.2", new URI("https://localhost:1234/page3"), "UserAgent")
      ),
      ("127.0.0.2", userSession5) -> Seq(
        new LogEntry(currentMillis + 2 * sessionTimeoutMillis + 1000, "127.0.0.2", new URI("https://localhost:1234/page4"), "UserAgent")
      )
    )

    val result = inputRdd.aggregateLogEntriesByUserSession.collect().toSeq

    result should contain theSameElementsAs expResult
  }

}
