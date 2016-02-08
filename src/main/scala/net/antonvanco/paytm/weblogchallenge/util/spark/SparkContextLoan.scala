package net.antonvanco.paytm.weblogchallenge.util.spark

import org.apache.spark
import org.apache.spark.SparkContext

/**
 * Created by antonvanco on 06/02/2016.
 *
 * Convenience trait which takes care of starting and stopping a SparkContext when used.
 * Useful for unit tests in which a SparkContext is created multiple times (only one SparkContext can exists in a single
 * JVM at any given time).
 */
trait SparkContextLoan {

  def withSparkContext[T](sparkConf: spark.SparkConf)(callback: SparkContext => T): T = {
    val sparkContext = new SparkContext(sparkConf)
    try {
      callback(sparkContext)
    } finally {
      sparkContext.stop()
    }
  }

}
