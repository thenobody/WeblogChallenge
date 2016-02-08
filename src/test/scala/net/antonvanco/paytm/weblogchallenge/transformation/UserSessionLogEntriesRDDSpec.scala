package net.antonvanco.paytm.weblogchallenge.transformation

import java.net.URI

import net.antonvanco.paytm.weblogchallenge.domain.{LogEntry, Session}
import net.antonvanco.paytm.weblogchallenge.util.spark.{SparkConf, SparkContextLoan}
import org.scalatest.{FlatSpec, Matchers}

/**
 * Created by antonvanco on 07/02/2016.
 */
class UserSessionLogEntriesRDDSpec extends FlatSpec with Matchers with SparkContextLoan {

  behavior of classOf[UserSessionLogEntriesRDD].getSimpleName

  val sparkConf = SparkConf(getClass.getSimpleName, "local[*]")

  val currentMillis = System.currentTimeMillis
  val sessionTimeoutMillis = 5000L

  val userSession1 = Session(1, currentMillis, currentMillis + 1000)
  val userSession2 = Session(2, currentMillis + sessionTimeoutMillis + 1000, currentMillis + sessionTimeoutMillis + 3000)

  val userSession3 = Session(1, currentMillis, currentMillis)
  val userSession4 = Session(2, currentMillis + sessionTimeoutMillis + 1000, currentMillis + sessionTimeoutMillis + 1000)
  val userSession5 = Session(3, currentMillis + 2 * sessionTimeoutMillis + 1000, currentMillis + 2 * sessionTimeoutMillis + 1000)
  val input = Seq(
    ("127.0.0.1", userSession1) -> Seq(
      new LogEntry(currentMillis, "127.0.0.1", new URI("https://localhost:1234/page1"), "UserAgent"),
      new LogEntry(currentMillis + 1000, "127.0.0.1", new URI("https://localhost:1234/page2"), "UserAgent")
    ),
    ("127.0.0.1", userSession2) -> Seq(
      new LogEntry(currentMillis + sessionTimeoutMillis + 1000, "127.0.0.1", new URI("https://localhost:1234/page3"), "UserAgent"),
      new LogEntry(currentMillis + sessionTimeoutMillis + 2000, "127.0.0.1", new URI("https://localhost:1234/page4"), "UserAgent"),
      new LogEntry(currentMillis + sessionTimeoutMillis + 3000, "127.0.0.1", new URI("https://localhost:1234/page4"), "UserAgent")
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

  it should "compute overall average session length" in withSparkContext(sparkConf) { sparkContext =>
    val expResult = (1000 + 2000 + 0 + 0 + 0) / 5
    val result = new UserSessionLogEntriesRDD(sparkContext.parallelize(input)).getAverageSessionLength

    result shouldEqual expResult
  }

  it should "generate lists of unique URIs per user session" in withSparkContext(sparkConf) { sparkContext =>
    val expResult = Map(
      ("127.0.0.1", userSession1) -> Set(
        new URI("https://localhost:1234/page1"),
        new URI("https://localhost:1234/page2")
      ),
      ("127.0.0.1", userSession2) -> Set(
        new URI("https://localhost:1234/page3"),
        new URI("https://localhost:1234/page4")
      ),
      ("127.0.0.2", userSession3) -> Set(new URI("https://localhost:1234/page1")),
      ("127.0.0.2", userSession4) -> Set(new URI("https://localhost:1234/page3")),
      ("127.0.0.2", userSession5) -> Set(new URI("https://localhost:1234/page4"))

    )
    val result = new UserSessionLogEntriesRDD(sparkContext.parallelize(input.toSeq)).getUniqueUrisBySession.collect()

    result should contain theSameElementsAs expResult
  }

  it should "order user sessions by unique URI count" in withSparkContext(sparkConf) { sparkContext =>
    val expResult = Seq(
      ("127.0.0.1", userSession1) -> 2,
      ("127.0.0.1", userSession2) -> 2,
      ("127.0.0.2", userSession3) -> 1,
      ("127.0.0.2", userSession4) -> 1,
      ("127.0.0.2", userSession5) -> 1
    )
    val result = new UserSessionLogEntriesRDD(sparkContext.parallelize(input.toSeq)).getUserSessionsOrderedByUniqueUriCount.collect().toSeq

    result shouldEqual expResult
  }

  it should "compute session lengths and sort users" in withSparkContext(sparkConf) { sparkContext =>
    val expResult = Seq(
      ("127.0.0.1", 2000L),
      ("127.0.0.1", 1000L),
      ("127.0.0.2", 0L),
      ("127.0.0.2", 0L),
      ("127.0.0.2", 0L)
    )

    val result = new UserSessionLogEntriesRDD(sparkContext.parallelize(input)).getUserSessionLengths.collect().toSeq

    result shouldEqual expResult
  }

  it should "order users by session length" in withSparkContext(sparkConf) { sparkContext =>
    val expResult = Seq(
      ("127.0.0.1", 2000L),
      ("127.0.0.2", 0L)
    )
    val result = new UserSessionLogEntriesRDD(sparkContext.parallelize(input.toSeq)).getUsersOrderedBySessionLength.collect().toSeq

    result shouldEqual expResult
  }

  it should "compute session counts for users" in withSparkContext(sparkConf) { sparkContext =>
    val expResult = Seq(
      ("127.0.0.2", 3L),
      ("127.0.0.1", 2L)
    )
    val result = new UserSessionLogEntriesRDD(sparkContext.parallelize(input.toSeq)).getSessionCountByUser.collect().toSeq

    result shouldEqual expResult
  }

  it should "get overall session count" in withSparkContext(sparkConf) { sparkContext =>
    val expResult = input.size
    val result = new UserSessionLogEntriesRDD(sparkContext.parallelize(input.toSeq)).getUserSessionCount

    result shouldEqual expResult
  }

  it should "get overall user count" in withSparkContext(sparkConf) { sparkContext =>
    val expResult = 2
    val result = new UserSessionLogEntriesRDD(sparkContext.parallelize(input.toSeq)).getUserCount

    result shouldEqual expResult
  }

  it should "compute overall average session count" in withSparkContext(sparkConf) { sparkContext =>
    val expResult = 5.0 / 2
    val result = new UserSessionLogEntriesRDD(sparkContext.parallelize(input.toSeq)).getAverageSessionCount

    result shouldEqual expResult
  }
}
