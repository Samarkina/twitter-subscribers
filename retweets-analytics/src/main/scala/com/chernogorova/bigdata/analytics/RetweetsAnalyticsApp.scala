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

//    val path = "src/main/resources/2021-01-06/received=20-55-34/"
    val path = "src/main/resources/2021-01-08/received=03-06-00/"

    val messageTablePath: String = path + "message/"
    val messageTableDF: DataFrame = ReadData.readParquetFile(spark, messageTablePath)

    val messageDirTablePath: String = path + "message_dir/"
    val messageDirTableDF: DataFrame = ReadData.readParquetFile(spark, messageDirTablePath)

    val retweetTablePath: String = path + "retweet/"
    val retweetTableDF: DataFrame = ReadData.readParquetFile(spark, retweetTablePath)

    retweetTableDF.show()

    val retweet2WaveTablePath: String = path + "retweet_second_wave/"
    val retweet2WaveTableDF: DataFrame = ReadData.readParquetFile(spark, retweet2WaveTablePath)

    val userDirTablePath: String = path + "user_dir/"
    val userDirTableDF: DataFrame = ReadData.readParquetFile(spark, userDirTablePath)

    val target: DataFrame = Analytics.createTargetTable(spark, userDirTableDF, messageDirTableDF, retweetTableDF)

    target.show()


    spark.stop()
  }
}
