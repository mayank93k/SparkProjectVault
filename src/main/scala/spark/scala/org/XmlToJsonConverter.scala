package spark.scala.org

import org.apache.log4j._
import org.apache.spark.sql.SparkSession

/**
 * Entry point of the XML Read application.
 *
 * This application demonstrates how to use Apache Spark to read XML data,
 * print the schema, display the data, and write it to a JSON file.
 */
object XmlToJsonConverter {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession.builder().appName("XML Read").master("local").getOrCreate()

    /**
     * Read the XML file into a DataFrame using the Databricks Spark XML library.
     * The "rowTag" option specifies the XML element to treat as a row in the DataFrame.
     */
    val df = spark.read.format("com.databricks.spark.xml").option("rowTag", "breakfast_menu")
      .load("src/main/resources/input/xmlfile/breakfast.xml")

    df.printSchema()
    df.show(false)

    // Write the contents of the DataFrame to a JSON file.
    df.write.json("src/main/resources/output/jsonfile/")
  }
}
