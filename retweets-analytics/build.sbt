name := "RetweetsAnalytics"

version := "0.1"

scalaVersion := "2.11.12"

libraryDependencies += "org.scalactic" %% "scalactic" % "3.1.0"
libraryDependencies += "org.scalatest" %% "scalatest" % "3.1.0" % "test"

val sparkVersion = "2.3.2"

// spark
libraryDependencies ++= Seq(
  "org.apache.spark" % "spark-core_2.11",
  "org.apache.spark" % "spark-streaming_2.11",
  "org.apache.spark" % "spark-sql_2.11"
) .map(_ % sparkVersion)