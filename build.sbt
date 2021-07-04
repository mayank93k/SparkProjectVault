name := "Project_Spark_Scala"

version := "0.1"

scalaVersion := "2.11.12"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "2.4.5",
  "com.typesafe" % "config" % "1.3.1",
  "org.apache.spark" %% "spark-streaming" % "2.4.5",
  "org.apache.spark" %% "spark-streaming-kafka" % "1.6.3",
  "org.apache.kafka" %% "kafka" % "0.10.1.0",
  "org.apache.spark" %% "spark-sql" % "2.4.5",
  "org.apache.spark" %% "spark-streaming-kafka-0-10" % "2.0.0"
)