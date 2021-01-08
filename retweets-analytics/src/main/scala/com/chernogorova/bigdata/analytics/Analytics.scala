package com.chernogorova.bigdata.analytics

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * Compute target table with the top ten users by a number of retweets.
 */
object Analytics {
  /**
   * Calculating the table with column "count" contains number of the retweets
   * @param spark: SparkSession
   * @param retweet: DataFrame with retweet table with USER_ID, SUBSCRIBER_ID, MESSAGE_ID columns
   * @return
   */
  def retweetCounting(spark: SparkSession,
                      retweet: DataFrame): DataFrame = {
    import spark.implicits._

    val count: DataFrame = retweet
      .select("USER_ID", "MESSAGE_ID")
      .groupBy("USER_ID", "MESSAGE_ID")
      .count()
      .sort($"count".desc)

    count
  }

  /**
   * Creating the target table with top 10 users by a number of the retweets.
   * Target table contains USER_ID, FIRST_NAME, LAST_NAME, MESSAGE_ID, TEXT, NUMBER_RETWEETS columns
   * @param spark: SparkSession
   * @param user_dir: Table with USER_ID, FIRST_NAME, LAST_NAME columns
   * @param message_dir: Table with MESSAGE_ID, TEXT columns
   * @param retweet: Table with USER_ID, SUBSCRIBER_ID, MESSAGE_ID columns
   * @return
   */
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
        $"rc.count".as("NUMBER_RETWEETS"))
      .orderBy($"rc.count".desc)
      .limit(10)

    target
  }
}
