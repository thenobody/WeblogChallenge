package net.antonvanco.paytm.weblogchallenge.util

/**
 * Created by antonvanco on 06/02/2016.
 */
object CsvUtil {
  val DefaultDelimiter = ','

  def parseCsv(line: String, delimiter: Char): List[String] = {
    def go(buffer: String, input: String, isQuote: Boolean, result: List[String]): List[String] = input.toList match {
      case '"' +: tail => tail match {
        case '"' +: t if isQuote => go(buffer + '"', t.mkString, isQuote, result)
        case _ => go(buffer, tail.mkString, !isQuote, result)
      }

      case `delimiter` +: tail if isQuote => go(buffer + delimiter, tail.mkString, isQuote, result)
      case `delimiter` +: tail => go("", tail.mkString, isQuote, result :+ buffer)

      case '\\' +: special +: tail if isQuote => go(buffer + parseSpecialChar(special), tail.mkString, isQuote, result)
      case '\\' +: _ => throw new IllegalArgumentException(s"""Invalid '\' character, line: $line""")

      case first +: tail => go(buffer + first, tail.mkString, isQuote, result)

      case Nil if isQuote => throw new IllegalArgumentException(s"""Missing closing '"' character, line: $line""")
      case Nil => result :+ buffer
    }

    if (line.isEmpty) List()
    else go("", line, isQuote = false, List())
  }

  def parseCsv(line: String): List[String] = parseCsv(line, DefaultDelimiter)

  def parseSpecialChar(special: Char): Char = special match {
    case 'n' => '\n'
    case 'r' => '\r'
    case invalid => throw new IllegalArgumentException(s"""Invalid special character '\$invalid'""")
  }

  def escapeField(value: String): String = value
    .replaceAll("\"", """""""")
    .replaceAll("\n", """\\n""")
    .replaceAll("\r", """\\r""")

  def toCsv(values: Iterable[String]): String = toCsv(values, DefaultDelimiter)
  def toCsv(values: Iterable[String], delimiter: Char): String = values.map(escapeField).map(field => s""""$field"""").mkString(delimiter.toString)
}
