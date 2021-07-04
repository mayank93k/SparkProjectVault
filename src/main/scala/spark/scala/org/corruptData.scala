package main.scala.scala.spark.practice.mayank.org

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import main.scala.org.spark.scala.mayank.InputOutputFileUtility
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object corruptData {
  System.setProperty("hadoop.home.dir", "C:\\winutils")

  case class WindowFunction(order_id: Int, order_date: String, customer_name: String, city: String, order_amount: Int)

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession.builder().master("local").getOrCreate()
    import spark.implicits._
    val readFile = spark.sparkContext.textFile(InputOutputFileUtility.getInputPath("test.csv")).toDF()
    readFile.show(false)


    val schema = new StructType().add("order_id", IntegerType, true)
      .add("order_date", StringType, true)
      .add("customer_name", StringType, true)
      .add("city", StringType, true)
      .add("order_amount", IntegerType, true)
      .add("_corrupt_record", StringType, true)

    val aa = spark.read.format("csv").option("header", "true").schema(schema).load(InputOutputFileUtility.getInputPath("test.csv"))
    aa.show(false)
    val header = readFile.first()
    println(header)

    val badRecord = aa.filter($"_corrupt_record".isNotNull).cache()
    val getCorruptData = badRecord.select("_corrupt_record").collect()
    getCorruptData.foreach(x => {
      val append = (x.mkString("") + "," + header.mkString("")).split(",")
      val a = Array("a", "b", "c", "d", "e")
      val b = append.flatMap { y => a.map { x => x.matches(y) } }
      if (b.contains(true)) {
        var count = 1
        for (i <- 0 until append.length) {
          if (!a.contains(append(i))) {
            count += 1
          }
          if (count > 1) {
            print(append(i) + " ")
            count = 1
          }
        }
      }
    })
  }
}
