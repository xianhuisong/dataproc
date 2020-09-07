package demo

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{udf, array, lit}

/**
 * the main class using Spark SQL DataFrame to process BigQuery data
 * input: BigQuery table, contains three feature columns: feature1, feature2,feature3
 * output: the predicting result by calling the cloud function model API
 */
object DataProcML {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("spark-bigquery-ml")
      .getOrCreate()

    val bucket = "dataproc-bq-demo"
    spark.conf.set("temporaryGcsBucket", bucket)
    //step 1: read BigQuery input table
    var df = BQData.loadData(spark)

    val cols = array("feature1", "feature2", "feature3")
    val sep = lit(",")
    //register UDFs
    val combineFeatures = udf(UDFs.allInOne _)
    val predictData = udf(UDFs.makeHttpCall _)
    df.show()
    //combine features values
    df = df.withColumn("param", combineFeatures(cols, sep))
    //call cloud function to get predicting result
    df = df.withColumn("label", predictData(df("param")))
    df.show()
    //save the DataFrame to the BigQuery result table
    BQData.saveData(df)
    spark.stop()
  }
}
