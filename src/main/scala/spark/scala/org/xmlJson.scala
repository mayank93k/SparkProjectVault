package spark.scala.org

import org.apache.log4j._
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext


object xmlJson {
  System.setProperty("hadoop.home.dir", "C:\\winutils");

  def main(args: Array[String]) {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val config = new SparkConf().setAppName("xmlJSON").setMaster("local")
    val sc = new SparkContext(config)
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)

    val df = sqlContext.read.format("com.databricks.spark.xml").option("rowTag", "breakfast_menu").
      load(InputOutputFileUtility.getInputPath("food.xml"))
    df.printSchema()
    df.show()

    df.write.format("org.apache.spark.sql.json").save(InputOutputFileUtility.getOutputPath("jsonOutput"))


  }

}
