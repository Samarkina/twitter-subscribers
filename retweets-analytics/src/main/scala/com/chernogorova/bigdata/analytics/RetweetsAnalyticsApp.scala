package com.chernogorova.bigdata.analytics
import org.apache.spark.sql.functions.col
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
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    val path = "src/main/resources/2021-01-06/received=20-55-34/"
//    val path = "src/main/resources/2021-01-08/received=16-44-36/"

    val messageTablePath: String = path + "message/"
    val messageTableDF: DataFrame = ReadData.readParquetFile(spark, messageTablePath)

    val messageDirTablePath: String = path + "message_dir/"
    val messageDirTableDF: DataFrame = ReadData.readParquetFile(spark, messageDirTablePath)

    val retweetTablePath: String = path + "retweet/"
    val retweetTableDF: DataFrame = ReadData.readParquetFile(spark, retweetTablePath)

    val retweet2WaveTablePath: String = path + "retweet_second_wave/"
    val retweet2WaveTableDF: DataFrame = ReadData.readParquetFile(spark, retweet2WaveTablePath)

    val userDirTablePath: String = path + "user_dir/"
    val userDirTableDF: DataFrame = ReadData.readParquetFile(spark, userDirTablePath)

    val firstWave: DataFrame = Analytics.createTargetTable(spark, userDirTableDF, messageDirTableDF, retweetTableDF)
    firstWave.show(false)

    val secondWave: DataFrame = Analytics.createTargetTable(spark, userDirTableDF, messageDirTableDF, retweet2WaveTableDF)
    secondWave.show(false)

    spark.stop()
  }
}
