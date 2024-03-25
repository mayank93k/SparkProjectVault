package spark.scala.org

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object xmlRead {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession.builder().appName("XML_Read").master("local").getOrCreate()

    val df1 = spark.read.format("com.databricks.spark.xml").option("rowTag", "CD").load(InputOutputFileUtility.getInputPath("cd.xml"))
    val df2 = spark.read.format("com.databricks.spark.xml").option("rowTag", "food").load(InputOutputFileUtility.getInputPath("food.xml"))
    val df3 = spark.read.format("com.databricks.spark.xml").option("rowTag", "PLANT").load(InputOutputFileUtility.getInputPath("plant.xml"))
    df1.printSchema()
    df1.show()
    df2.printSchema()
    df2.show()
    df3.printSchema()
    df3.show()
    df1.write.format("json").save(InputOutputFileUtility.getOutputPath("xmlCD"))
    df2.write.format("json").save(InputOutputFileUtility.getOutputPath("xmlFOOD"))
    df3.write.format("json").save(InputOutputFileUtility.getOutputPath("xmlPLANT"))
  }
}