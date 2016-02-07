package net.antonvanco.paytm.weblogchallenge

import net.antonvanco.paytm.weblogchallenge.transformation._
import net.antonvanco.paytm.weblogchallenge.util.spark.{SparkConf, SparkContextLoan}
import org.slf4j.LoggerFactory

import scala.collection.immutable.ListMap

/**
 * Created by antonvanco on 06/02/2016.
 */
object Main extends SparkContextLoan {
  val logger = LoggerFactory.getLogger(getClass)

  def main(args: Array[String]): Unit = withSparkContextLoan(SparkConf(getClass.getSimpleName, "local[*]")) { sparkContext =>
    val userSessionsRdd = container.SessioniseLogEntriesTaskInstance
      .execute("data/2015_07_22_mktplace_shop_web_log_sample.log.gz", sparkContext)
      .cache()

    // average session time
    val avgSessionLength = userSessionsRdd.getAverageSessionLength

    // unique url count per session
    val topSessionUrisMap = ListMap(
      userSessionsRdd.getUserSessionsOrderedByUniqueUriCount.take(container.topSessionsByUriCount): _*
    )

    // users with longest session times
    val longestUserSessionsMap = ListMap(
      userSessionsRdd.getUsersOrderedBySessionLength.take(container.topUsersBySessionLength): _*
    )

    // additional metrics
    val userCount = userSessionsRdd.getUserCount
    val userSessionsCounts = ListMap(
      userSessionsRdd.getSessionCountByUser.take(container.topUsersBySessionCount): _*
    )
    val avgUserSessionCount = userSessionsRdd.getAverageSessionCount

    logger.info(s"avgUserSessionCount: $avgUserSessionCount")
    logger.info(s"avgSessionLength: $avgSessionLength")
    logger.info(s"longestUserSessions:\n${longestUserSessionsMap.mkString("\n")}")

    logger.info(s"topSessionUriCounts:\n${topSessionUrisMap.mkString("\n")}")

    logger.info(s"userCount: $userCount")
    logger.info(s"topSessionCounts:\n${userSessionsCounts.mkString("\n")}")
  }
}
