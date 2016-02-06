package net.antonvanco.paytm.weblogchallenge.util.spark

import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by antonvanco on 06/02/2016.
 */
trait SparkContextLoan {

  def withSparkContextLoan[T](sparkConf: SparkConf)(callback: SparkContext => T): T = {
    val sparkContext = new SparkContext(sparkConf)
    try {
      callback(sparkContext)
    } finally {
      sparkContext.stop()
    }
  }

}
