package spark.scala.org

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * About: Read input as JSON and write as CSV file
  */
object jsonRead {
  System.setProperty("hadoop.home.dir", "C:\\winutils")
  def main(args: Array[String]) {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val config = new SparkConf().setAppName("xmlJSON").setMaster("local")
    val sc = new SparkContext(config)
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    val df1 = sqlContext.read.format("org.apache.spark.sql.json").load(InputOutputFileUtility.getInputPath("cd.json"))
    val df2 = sqlContext.read.format("org.apache.spark.sql.json").load(InputOutputFileUtility.getInputPath("food.json"))
    val df3 = sqlContext.read.format("org.apache.spark.sql.json").load(InputOutputFileUtility.getInputPath("plant.json"))
    df1.printSchema()
    df1.show()
    df2.printSchema()
    df2.show()
    df3.printSchema()
    df3.show()
    df1.write.format("csv").save(InputOutputFileUtility.getOutputPath("cdCSV"))
    df2.write.format("csv").save(InputOutputFileUtility.getOutputPath("foodCSV"))
    df3.write.format("csv").save(InputOutputFileUtility.getOutputPath("plantCSV"))
  }
}