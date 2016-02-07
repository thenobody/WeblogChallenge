package net.antonvanco.paytm.weblogchallenge

import com.typesafe.config.ConfigFactory
import net.antonvanco.paytm.weblogchallenge.transformation.SessioniseLogEntriesTask

/**
 * Created by antonvanco on 07/02/2016.
 */
package object container {

  val config = ConfigFactory.load().resolve()

  val sessionTimeoutMillis = config.getLong("paytm.weblogchallenge.session.timeoutMillis")

  val topSessionsByUriCount = config.getInt("paytm.weblogchallenge.metrics.topRankings.sessionsByUriCount")
  val topUsersBySessionLength = config.getInt("paytm.weblogchallenge.metrics.topRankings.usersBySessionLength")
  val topUsersBySessionCount = config.getInt("paytm.weblogchallenge.metrics.topRankings.usersBySessionCount")

  object SessioniseLogEntriesTaskInstance extends SessioniseLogEntriesTask(sessionTimeoutMillis)

}
