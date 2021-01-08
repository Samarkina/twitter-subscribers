package com.chernogorova.bigdata.analytics

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

object Analytics {

  def retweetCounting(spark: SparkSession,
                      retweet: DataFrame): DataFrame = {
    import spark.implicits._

    val count: DataFrame = retweet
      .select("USER_ID", "MESSAGE_ID")
      .groupBy("USER_ID", "MESSAGE_ID")
      .count()
      .sort($"count".desc)
    count.show()

    count
  }

  def createTargetTable(spark: SparkSession,
                        user_dir: DataFrame,
                        message_dir: DataFrame,
                        retweet: DataFrame): DataFrame = {
    import spark.implicits._

    val retweetCount: DataFrame = retweetCounting(spark, retweet)

    val target: DataFrame = retweetCount.as("rc")
      .join(
        user_dir.as("ud"),
        col("rc.USER_ID") === col("ud.USER_ID"),
        "INNER")
      .join(
        message_dir.as("md"),
        col("md.MESSAGE_ID") === col("rc.MESSAGE_ID"),
      "INNER")
      .select($"ud.USER_ID",
        $"ud.FIRST_NAME",
        $"ud.LAST_NAME",
        $"md.MESSAGE_ID",
        $"md.TEXT",
        $"rc.count")


    target


  }

}
