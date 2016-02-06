package net.antonvanco.paytm.weblogchallenge.util.spark

import org.apache.spark

/**
 * Created by antonvanco on 06/02/2016.
 */
object SparkConf {

  val DefaultMaster = "local[*]"

  def apply(applicationName: String, master: String): spark.SparkConf = new spark.SparkConf()
    .setAppName(applicationName)
    .setMaster(master)

  def apply(applicationName: String): spark.SparkConf = SparkConf(applicationName, DefaultMaster)
}
