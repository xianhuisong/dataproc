package demo

import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

/**
 * util class to read and write DataFrame to BigQuery tables
 */
object BQData {
  def loadData(spark: SparkSession): DataFrame = {
    spark.read.format("bigquery")
      .option("table", "dataproc-example-286207:dataproc_demo_ds.dataproc_input_tbl")
      .load()
      .cache()
  }

  def saveData(df: DataFrame) = {
    df.write.format("bigquery").mode(SaveMode.Overwrite)
      .option("table", "dataproc_demo_ds.dataproc_output_tbl")
      .save()
  }
}
