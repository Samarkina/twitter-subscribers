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

    val messageTablePath: String = path + "message/"
    val messageTableDF: DataFrame = spark.read.parquet(messageTablePath)

    val messageDirTablePath: String = path + "message_dir/"
    val messageDirTableDF: DataFrame = spark.read.parquet(messageDirTablePath)

    val retweetTablePath: String = path + "retweet/"
    val retweetTableDF: DataFrame = spark.read.parquet(retweetTablePath)

    val retweet2WaveTablePath: String = path + "retweet_second_wave/"
    val retweet2WaveTableDF: DataFrame = spark.read.parquet(retweet2WaveTablePath)

    val userDirTablePath: String = path + "user_dir/"
    val userDirTableDF: DataFrame = spark.read.parquet(userDirTablePath)

    val firstWave: DataFrame = Analytics.createTargetTable(spark, userDirTableDF, messageDirTableDF, retweetTableDF)
    firstWave.show(false)

    val secondWave: DataFrame = Analytics.createTargetTable(spark, userDirTableDF, messageDirTableDF, retweet2WaveTableDF)
    secondWave.show(false)

    spark.stop()
  }
}
