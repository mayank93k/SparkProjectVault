package spark.scala.org

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

/**
 * About: Read input as JSON and write as CSV file
 */
object JsonRead {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession.builder().appName("Patient").master("local").getOrCreate()

    val df1 = spark.read.format("org.apache.spark.sql.json").load(InputOutputFileUtility.getInputPath("cd.json"))
    val df2 = spark.read.format("org.apache.spark.sql.json").load(InputOutputFileUtility.getInputPath("food.json"))
    val df3 = spark.read.format("org.apache.spark.sql.json").load(InputOutputFileUtility.getInputPath("plant.json"))
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