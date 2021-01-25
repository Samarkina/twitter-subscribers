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

//    val path = "src/main/resources/2021-01-06/received=20-55-34/"
//    val path = "src/main/resources/received=2021-01-24/time=00-38-10/" // big
    val path = "src/main/resources/received=2021-01-24/time=00-40-58/" // super small
//    val path = "src/main/resources/received=2021-01-25/time=01-27-51/" // small

    val messageTablePath: String = path + "message/"
    val messageTableDF: DataFrame = spark.read.parquet(messageTablePath)

    val messageDirTablePath: String = path + "message_dir/"
    val messageDirTableDF: DataFrame = spark.read.parquet(messageDirTablePath)

    val retweetTablePath: String = path + "retweet/"
    val retweetTableDF: DataFrame = spark.read.parquet(retweetTablePath)

    val userDirTablePath: String = path + "user_dir/"
    val userDirTableDF: DataFrame = spark.read.parquet(userDirTablePath)

    val firstWave: DataFrame = Analytics.createTableForFirstWave(spark, userDirTableDF, messageTableDF, messageDirTableDF, retweetTableDF)
    firstWave.show(false)

//    val secondWave: DataFrame = Analytics.createTargetTable(spark, userDirTableDF, messageDirTableDF, retweetSecondWaveTableDF)
//    secondWave.show(false)

    spark.stop()
  }
}
