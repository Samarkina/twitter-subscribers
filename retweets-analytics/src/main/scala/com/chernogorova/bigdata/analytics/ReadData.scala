package com.chernogorova.bigdata.analytics

import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * Contains utility functions for reading files.
 */
object ReadData {

  /**
   * Method reading Parquet file from given directory
   * @param spark SparkSession
   * @param filePath Path to file for reading
   * @return DataFrame with content of the Parquet file
   */
  def readParquetFile(spark: SparkSession, filePath: String): DataFrame = {
    import spark.implicits._

    val parquetFileDF = spark.read.parquet(filePath)

    parquetFileDF
  }

}
