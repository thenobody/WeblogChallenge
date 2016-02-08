package net.antonvanco.paytm.weblogchallenge

import net.antonvanco.paytm.weblogchallenge.transformation._
import net.antonvanco.paytm.weblogchallenge.util.spark.{SparkConf, SparkContextLoan}
import org.slf4j.LoggerFactory

import scala.collection.immutable.ListMap

/**
 * Created by antonvanco on 06/02/2016.
 *
 * This is the main entry point for the assignment.
 *
 * Here we create the SparkContext and process the input ELB log data (from "data/2015_07_22_mktplace_shop_web_log_sample.log.gz").
 * Afterwards, we generate the metrics:
 * - average user session count
 * - average user session length (duration)
 * - top N users with the longest session lengths
 * - top N user sessions with the highest count of unique URIs
 * - overall unique user count
 * - top N user sessions with the highest number of sessions
 *
 * See src/main/resources/application.conf and container/package.object for config parameters and their usage
 */
object Main extends SparkContextLoan {
  val logger = LoggerFactory.getLogger(getClass)
  val inputPath = "data/2015_07_22_mktplace_shop_web_log_sample.log.gz"

  def main(args: Array[String]): Unit = withSparkContext(SparkConf(getClass.getSimpleName, "local[*]")) { sparkContext =>
    val userSessionsRdd = container.SessioniseLogEntriesTaskInstance
      .execute(inputPath, sparkContext)
      .cache()
    // caching the result since we're going to use it multiple times (for individual metrics computations)

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

    logger.info("RESULTS")
    logger.info("---------------------")
    logger.info(s"avgUserSessionCount: $avgUserSessionCount")
    logger.info(s"avgSessionLength: $avgSessionLength")
    logger.info(s"longestUserSessions:\n${longestUserSessionsMap.mkString("\n")}")

    logger.info(s"topSessionUriCounts:\n${topSessionUrisMap.mkString("\n")}")

    logger.info(s"userCount: $userCount")
    logger.info(s"topSessionCounts:\n${userSessionsCounts.mkString("\n")}")
  }
}
