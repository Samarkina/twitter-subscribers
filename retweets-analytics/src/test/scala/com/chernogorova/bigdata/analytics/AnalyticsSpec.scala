package com.chernogorova.bigdata.analytics

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class AnalyticsSpec extends AnyWordSpec with Matchers with SparkContextSetup {
  val path = "src/test/resources/2021-01-08/received=16-44-36/"
  val retweetTablePath: String = path + "retweet/"
  val messageDirTablePath: String = path + "message_dir/"
  val userDirTablePath: String = path + "user_dir/"

  "Calculating the number of the retweets" in withSparkContext { spark =>
    val retweet: DataFrame = ReadData.readParquetFile(spark, retweetTablePath)

    val answer: DataFrame = Analytics.retweetCounting(spark, retweet)
    val correct_answer: DataFrame = createRetweetCountTable(spark)
    answer.collect() should contain theSameElementsAs (correct_answer.collect())

  }

  "Creating the target table with top 10 user by a number of the retweets" in withSparkContext {spark =>
    val retweet: DataFrame = ReadData.readParquetFile(spark, retweetTablePath)
    val messageDir: DataFrame = ReadData.readParquetFile(spark, messageDirTablePath)
    val userDir: DataFrame = ReadData.readParquetFile(spark, userDirTablePath)

    val answer: DataFrame = Analytics.createTargetTable(spark, userDir, messageDir, retweet)
    val correct_answer: DataFrame = createTargetTable(spark)
    answer.collect() should contain theSameElementsAs (correct_answer.collect())

  }


  def createRetweetCountTable(spark: SparkSession): DataFrame = {
    import spark.implicits._

    val df = Seq(
      (18, 18, 2),
      (12, 9, 2),
      (13, 19, 2),
      (7, 16, 2),
      (6, 24, 1),
      (17, 8, 1),
      (8, 1, 1),
      (19, 28, 1),
      (1, 27, 1),
      (1, 14, 1),
      (16, 11, 1),
      (14 ,17, 1),
      (0, 6, 1),
      (7, 7, 1),
      (11, 25, 1),
      (8, 23, 1)
    ).toDF("USER_ID", "MESSAGE_ID", "count")

    df
  }

  def createTargetTable(spark: SparkSession): DataFrame = {
    import spark.implicits._

    val df = Seq(
      (7, "Ava", "Martin", 16, "metoqyvuhvdzrhzfiefnxcxf", 2),
      (18, "Benjamin", "Nguyen", 18, "vlxdv", 2),
      (12, "Harrison", "Nguyen", 9, "ohselquuvnayyobchbsxdw"  , 2),
      (13, "Jose", "Brown", 19, "ewwrm", 2),
      (6, "Parker", "Miller", 24, "inymdzfrfrjiybpykyeaiyerv", 1),
      (17, "Mia", "Robinson", 8, "ygxmniwsilyetnbs", 1),
      (8, "Ayden", "Campbell", 1, "kyxzq", 1),
      (1, "Bennett", "Baker", 27, "zwfawgheardyx", 1),
      (1, "Bennett", "Baker", 14, "dbdhahvxy", 1),
      (16, "Kayden", "Miller", 11, "extdqvvcgitukfzwys", 1)
    ).toDF("USER_ID", "FIRST_NAME", "LAST_NAME", "MESSAGE_ID", "TEXT", "NUMBER_RETWEETS")

    df

  }
}
