package com.chernogorova.bigdata.analytics

import org.apache.spark.sql.SparkSession

trait SparkContextSetup {
  def withSparkContext(testMethod: (SparkSession) => Any) {
    val spark = SparkSession
      .builder()
      .appName("Spark SQL")
      .config("spark.master", "local")
      .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    try {
      testMethod(spark)
    }
    finally spark.stop()
  }
}
