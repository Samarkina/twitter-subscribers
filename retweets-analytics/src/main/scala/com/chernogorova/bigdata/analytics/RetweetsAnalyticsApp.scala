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

    val path = "src/main/resources/received=2021-01-24/time=00-38-10/"

    val messageTablePath: String = path + "message/"
    val messageTableDF: DataFrame = spark.read.parquet(messageTablePath)

    val messageDirTablePath: String = path + "message_dir/"
    val messageDirTableDF: DataFrame = spark.read.parquet(messageDirTablePath)

    val retweetTablePath: String = path + "retweet/"
    val retweetTableDF: DataFrame = spark.read.parquet(retweetTablePath)

    val userDirTablePath: String = path + "user_dir/"
    val userDirTableDF: DataFrame = spark.read.parquet(userDirTablePath)

    val allData: DataFrame = Analytics.createTableForAllWaves(spark, userDirTableDF, messageTableDF, messageDirTableDF, retweetTableDF)

    val firstWave: DataFrame = Analytics.createTableForFirstWave(spark, allData, messageTableDF)
    firstWave.show(false)

    val secondWave: DataFrame = Analytics.createTableForSecondWave(spark, allData, userDirTableDF, messageTableDF)
    secondWave.show(false)

    spark.stop()
  }
}
