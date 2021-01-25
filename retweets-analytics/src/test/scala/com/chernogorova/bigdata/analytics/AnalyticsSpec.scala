package com.chernogorova.bigdata.analytics

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class AnalyticsSpec extends AnyWordSpec with Matchers with SparkContextSetup {
  val path = "src/main/resources/received=2021-01-24/time=00-40-58/"
  val retweetTablePath: String = path + "retweet/"
  val messageDirTablePath: String = path + "message_dir/"
  val messageTablePath: String = path + "message/"
  val userDirTablePath: String = path + "user_dir/"

  "Calculating the number of the retweets" in withSparkContext { spark =>
    val retweet: DataFrame = spark.read.parquet(retweetTablePath)

    val actual: DataFrame = Analytics.countRetweets(spark, retweet)
    val expected: DataFrame = createExpectedRetweetCountTable(spark)

    actual.collect() should contain theSameElementsAs (expected.collect())

  }

  "Creating the all data table" in withSparkContext {spark =>
    val retweet: DataFrame = spark.read.parquet(retweetTablePath)
    val messageDir: DataFrame = spark.read.parquet(messageDirTablePath)
    val message: DataFrame = spark.read.parquet(messageTablePath)
    val userDir: DataFrame = spark.read.parquet(userDirTablePath)

    val actual: DataFrame = Analytics.createTableForAllWaves(spark, userDir, message, messageDir, retweet)
    val expected: DataFrame = createExpectedAllDataTable(spark)

    actual.collect() should contain theSameElementsAs (expected.collect())

  }

  "Creating the target table with top 10 user by a number of the retweets for First Wave" in withSparkContext {spark =>
    val retweet: DataFrame = spark.read.parquet(retweetTablePath)
    val messageDir: DataFrame = spark.read.parquet(messageDirTablePath)
    val message: DataFrame = spark.read.parquet(messageTablePath)
    val userDir: DataFrame = spark.read.parquet(userDirTablePath)
    val allData: DataFrame = Analytics.createTableForAllWaves(spark, userDir, message, messageDir, retweet)

    val actual: DataFrame = Analytics.createTableForFirstWave(spark, allData, message)
    val expected: DataFrame = createExpectedTargetTableforFirstWave(spark)

    actual.collect() should contain theSameElementsAs (expected.collect())

  }

  "Creating the target table with top 10 user by a number of the retweets for Second Wave" in withSparkContext {spark =>
    val retweet: DataFrame = spark.read.parquet(retweetTablePath)
    val messageDir: DataFrame = spark.read.parquet(messageDirTablePath)
    val message: DataFrame = spark.read.parquet(messageTablePath)
    val userDir: DataFrame = spark.read.parquet(userDirTablePath)
    val allData: DataFrame = Analytics.createTableForAllWaves(spark, userDir, message, messageDir, retweet)

    val actual: DataFrame = Analytics.createTableForSecondWave(spark, allData, userDir, message)
    val expected: DataFrame = createExpectedTargetTableforSecondWave(spark)

    actual.collect() should contain theSameElementsAs (expected.collect())

  }


  def createExpectedRetweetCountTable(spark: SparkSession): DataFrame = {
    import spark.implicits._

    val df = Seq(
      (1, 5, 2),
      (5, 6, 1),
      (6, 17, 1),
      (2, 17, 1),
      (8, 5, 1),
      (1, 13, 1)
    ).toDF("USER_ID", "MESSAGE_ID", "count")

    df
  }

  def createExpectedAllDataTable(spark: SparkSession): DataFrame = {
    import spark.implicits._

    val df = Seq(
      (1, "Evan", "Flores", 5, "mlqu", 2),
      (6, "Declan", "Rivera", 17, "pmjlooa", 1),
      (5, "Mia", "Walker", 6, "hhwurmhqiyzlm", 1),
      (2, "Silas", "Anderson", 17, "pmjlooa", 1),
      (8, "Bennett", "Rodriguez", 5, "mlqu", 1),
      (1, "Evan", "Flores", 13, "inkygdpybk", 1)
    ).toDF("USER_ID", "FIRST_NAME", "LAST_NAME", "MESSAGE_ID", "TEXT", "NUMBER_RETWEETS")

    df
  }

  def createExpectedTargetTableforFirstWave(spark: SparkSession): DataFrame = {
    import spark.implicits._

    val df = Seq(
      (1, "Evan", "Flores", 5, "mlqu", 2),
      (5, "Mia", "Walker", 6, "hhwurmhqiyzlm", 1),
      (2, "Silas", "Anderson", 17, "pmjlooa", 1),
      (1, "Evan", "Flores", 13, "inkygdpybk", 1)
    ).toDF("USER_ID", "FIRST_NAME", "LAST_NAME", "MESSAGE_ID", "TEXT", "NUMBER_RETWEETS")

    df
  }

  def createExpectedTargetTableforSecondWave(spark: SparkSession): DataFrame = {
    import spark.implicits._

    val df = Seq(
      (1, "Evan", "Flores", 5, "mlqu", 1),
      (2, "Silas", "Anderson", 17, "pmjlooa", 1)
    ).toDF("USER_ID", "FIRST_NAME", "LAST_NAME", "MESSAGE_ID", "TEXT", "NUMBER_RETWEETS")

    df
  }
}
