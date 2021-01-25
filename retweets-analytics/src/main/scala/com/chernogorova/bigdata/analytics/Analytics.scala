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
  def countRetweets(spark: SparkSession,
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
   * Creating the target table with top 10 users by a number of the retweets for first wave.
   *
   * @param spark: SparkSession
   * @param user_dir: Table with USER_ID, FIRST_NAME, LAST_NAME columns
   * @param message: Table with USER_ID, MESSAGE_ID columns
   * @param message_dir: Table with MESSAGE_ID, TEXT columns
   * @param retweet: Table with USER_ID, SUBSCRIBER_ID, MESSAGE_ID columns
   * @return Target table contains USER_ID, FIRST_NAME, LAST_NAME, MESSAGE_ID, TEXT, NUMBER_RETWEETS columns
   */
  def createTableForFirstWave(spark: SparkSession,
                              user_dir: DataFrame,
                              message: DataFrame,
                              message_dir: DataFrame,
                              retweet: DataFrame): DataFrame = {

    import spark.implicits._
    val retweetCount: DataFrame = countRetweets(spark, retweet)

    val all_data: DataFrame = retweetCount.as("rc")
      .join(
        user_dir.as("ud"),
        col("ud.USER_ID") === col("rc.USER_ID"),
        "INNER"
      )
      .join(
        message_dir.as("mess_dir"),
        col("mess_dir.MESSAGE_ID") === col("rc.MESSAGE_ID"),
        "INNER"
      )
      .select($"rc.USER_ID",
        $"ud.FIRST_NAME",
        $"ud.LAST_NAME",
        $"rc.MESSAGE_ID",
        $"mess_dir.TEXT",
        $"rc.count".as("NUMBER_RETWEETS")
      )

    val first_wave: DataFrame = all_data.as("t")
      .join(
        message.as("mess"),
        col("mess.MESSAGE_ID") === col("t.MESSAGE_ID") &&
          col("mess.USER_ID") === col("t.USER_ID"),
        "INNER"
      )
      .select($"t.USER_ID",
        $"t.FIRST_NAME",
        $"t.LAST_NAME",
        $"t.MESSAGE_ID",
        $"t.TEXT",
        $"t.NUMBER_RETWEETS"
      )
      .orderBy($"t.NUMBER_RETWEETS".desc)
      .limit(10)

    first_wave
  }

}
