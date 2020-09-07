package demo

/**
 * UDFs used in the Spark SQL transformations
 */
object UDFs {
  val endpoint = "https://us-central1-dataproc-example-286207.cloudfunctions.net/my-test-func"

  /**
   * call cloud function to get the model predicting result
   * @param input a two-dimension array
   * @return the array of predicting result
   */
  def makeHttpCall(input: String) = {
    import scalaj.http._
    val response: HttpResponse[String] = Http(endpoint).param("features", input).asString
    response.body
  }

  /**
   * combine all the features into one column
   * @param seq feature columns
   * @param sep separator
   * @return combined value
   */
  def allInOne(seq: Seq[Any], sep: String): String = seq.mkString(sep)
}
