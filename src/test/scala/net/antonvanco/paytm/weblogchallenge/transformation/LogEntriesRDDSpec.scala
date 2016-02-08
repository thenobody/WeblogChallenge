package net.antonvanco.paytm.weblogchallenge.transformation

import java.net.URI

import com.github.nscala_time.time.Imports._
import net.antonvanco.paytm.weblogchallenge.domain.LogEntry
import net.antonvanco.paytm.weblogchallenge.util.spark._
import org.scalatest.{Matchers, FlatSpec}

/**
 * Created by antonvanco on 07/02/2016.
 */
class LogEntriesRDDSpec extends FlatSpec with Matchers with SparkContextLoan {

  behavior of classOf[LogEntriesRDD].getSimpleName

  val sparkConf = SparkConf(getClass.getSimpleName, "local[*]")

  it should "parse log entries from ELB log lines" in withSparkContext(sparkConf) { sparkContext =>
    val linesRdd = sparkContext.parallelize(Seq(
      """2015-07-22T09:00:28.019143Z marketpalce-shop 192.168.1.1:54635 10.0.6.158:80 0.000022 0.026109 0.00002 200 200 0 699 "GET https://paytm.com:443/shop/authresponse?code=f2405b05-e2ee-4b0d-8f6a-9fed0fcfe2e0&state=null HTTP/1.1" "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/43.0.2357.130 Safari/537.36" ECDHE-RSA-AES128-GCM-SHA256 TLSv1.2""",
      """2015-07-22T09:10:43.345123Z marketpalce-shop 192.168.1.1:54635 10.0.6.158:80 0.000022 0.026109 0.00002 200 200 0 699 "POST https://paytm.com:443/shop/authresponse?code=f2405b05-e2ee-4b0d-8f6a-9fed0fcfe2e0&state=null HTTP/1.0" "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/43.0.2357.130 Safari/537.36" ECDHE-RSA-AES128-GCM-SHA256 TLSv1.2""",
      """2015-07-22T09:14:03.984578Z marketpalce-shop 192.168.1.2:8080 10.0.6.158:80 0.000022 0.026109 0.00002 200 200 0 699 "GET https://paytm.com:443/shop/authresponse?code=f2405b05-e2ee-4b0d-8f6a-9fed0fcfe2e0&state=null HTTP/1.1" "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/43.0.2357.130 Safari/537.36" ECDHE-RSA-AES128-GCM-SHA256 TLSv1.2""",
      """2015-07-22T09:15:58.748391Z marketpalce-shop 192.168.1.2:1234 10.0.6.158:80 0.000022 0.026109 0.00002 200 200 0 699 "GET https://paytm.com:443/shop/authresponse?code=f2405b05-e2ee-4b0d-8f6a-9fed0fcfe2e0&invalid={{ INVALID }}&state=null HTTP/1.1" "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/43.0.2357.130 Safari/537.36" ECDHE-RSA-AES128-GCM-SHA256 TLSv1.2"""
    ))

    val expResult = Seq(
      "192.168.1.1:54635" -> new LogEntry(
        DateTime.parse("2015-07-22T09:00:28.019143Z").getMillis,
        "192.168.1.1:54635",
        new URI("https://paytm.com:443/shop/authresponse?code=f2405b05-e2ee-4b0d-8f6a-9fed0fcfe2e0&state=null"),
        "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/43.0.2357.130 Safari/537.36"
      ),
      "192.168.1.1:54635" -> new LogEntry(
        DateTime.parse("2015-07-22T09:10:43.345123Z").getMillis,
        "192.168.1.1:54635",
        new URI("https://paytm.com:443/shop/authresponse?code=f2405b05-e2ee-4b0d-8f6a-9fed0fcfe2e0&state=null"),
        "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/43.0.2357.130 Safari/537.36"
      ),
      "192.168.1.2:8080" -> new LogEntry(
        DateTime.parse("2015-07-22T09:14:03.984578Z").getMillis,
        "192.168.1.2:8080",
        new URI("https://paytm.com:443/shop/authresponse?code=f2405b05-e2ee-4b0d-8f6a-9fed0fcfe2e0&state=null"),
        "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/43.0.2357.130 Safari/537.36"
      )
    )

    val result = linesRdd.parseRawLogsToLogEntries.collect().toSeq

    result shouldEqual expResult
  }
}
