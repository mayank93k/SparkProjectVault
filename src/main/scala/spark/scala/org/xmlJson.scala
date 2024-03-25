package spark.scala.org

import org.apache.log4j._
import org.apache.spark.sql.SparkSession


object xmlJson {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession.builder().appName("XML_Read").master("local").getOrCreate()

    val df = spark.read.format("com.databricks.spark.xml").option("rowTag", "breakfast_menu").
      load(InputOutputFileUtility.getInputPath("food.xml"))

    df.printSchema()
    df.show()

    df.write.format("json").save(InputOutputFileUtility.getOutputPath("jsonOutput"))


  }

}
