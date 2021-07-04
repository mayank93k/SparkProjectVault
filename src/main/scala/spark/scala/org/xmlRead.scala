package spark.scala.org

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

object xmlRead {
  System.setProperty("hadoop.home.dir", "C:\\winutils");
  def main(args: Array[String]) {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val config = new SparkConf().setAppName("xmlJSON").setMaster("local")
    val sc = new SparkContext(config)
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)

    val df1 = sqlContext.read.format("com.databricks.spark.xml").option("rowTag","CD").load(InputOutputFileUtility.getInputPath("cd.xml"))
    val df2 = sqlContext.read.format("com.databricks.spark.xml").option("rowTag","food").load(InputOutputFileUtility.getInputPath("food.xml"))
    val df3 = sqlContext.read.format("com.databricks.spark.xml").option("rowTag","PLANT").load(InputOutputFileUtility.getInputPath("plant.xml"))
    df1.printSchema()
    df1.show()
    df2.printSchema()
    df2.show()
    df3.printSchema()
    df3.show()
    df1.write.format("org.apache.spark.sql.json").save(InputOutputFileUtility.getOutputPath("xmlCD"))
    df2.write.format("org.apache.spark.sql.json").save(InputOutputFileUtility.getOutputPath("xmlFOOD"))
    df3.write.format("org.apache.spark.sql.json").save(InputOutputFileUtility.getOutputPath("xmlPLANT"))
  }
}