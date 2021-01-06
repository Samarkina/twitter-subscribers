package com.chernogorova.bigdata.analytics
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * Retweets Analytics App
 */
object RetweetsAnalyticsApp {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("RetweetsAnalyticsApp")
      .config("spark.master", "local[2]")
      .config("spark.driver.bindAddress", "127.0.0.1")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    val messageTablePath: String = "src/main/resources/2021-01-06/received=20-55-34/message/"
    val messageTableDF: DataFrame = ReadData.readParquetFile(spark, messageTablePath)

    messageTableDF.show()

    spark.stop()
  }
}
