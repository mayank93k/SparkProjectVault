name := "Project_Spark_Scala"

version := "0.1"

scalaVersion := "2.12.12"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "2.4.8",
  "com.typesafe" % "config" % "1.4.3",
  "org.apache.spark" %% "spark-sql" % "2.4.8",
  "com.typesafe.scala-logging" %% "scala-logging" % "3.7.2",
  "com.google.cloud.spark" %% "spark-bigquery-with-dependencies" % "0.32.2",
  "org.scalatest" %% "scalatest" % "3.2.9" % Test,
  "za.co.absa.cobrix" %% "spark-cobol" % "2.0.2",
  "com.databricks" %% "spark-xml" % "0.15.0"
)

excludeDependencies ++= Seq(ExclusionRule("ch.qos.logoback", "logoback-classic"))
