package net.antonvanco.paytm.weblogchallenge.util

import org.scalatest.{Matchers, FlatSpec}

/**
 * Created by antonvanco on 07/02/2016.
 */
class CsvUtilSpec extends FlatSpec with Matchers {

  behavior of CsvUtil.getClass.getSimpleName

  it should "parse a CSV line" in {
    val input = """no quotes,"quoted",123123,,"there should a "" here""""
    val expResult = List("no quotes", "quoted", "123123", "", """there should a " here""")

    val result = CsvUtil.parseCsv(input, ',')

    result shouldEqual expResult
  }

  it should "transform a list of strings into a CSV line" in {
    val input = Seq("value", "123123", "", """there should a " here""")
    val expResult = """"value","123123","","there should a "" here""""

    val result = CsvUtil.toCsv(input, ',')

    result shouldEqual expResult
  }

}
