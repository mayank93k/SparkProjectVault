package spark.scala.org

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._

object CorruptData {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession.builder().master("local").getOrCreate()
    import spark.implicits._
    val readFile = spark.sparkContext.textFile(InputOutputFileUtility.getInputPath("test.csv")).toDF()
    readFile.show(false)

    val schema = new StructType().add("order_id", IntegerType, nullable = true)
      .add("order_date", StringType, nullable = true)
      .add("customer_name", StringType, nullable = true)
      .add("city", StringType, nullable = true)
      .add("order_amount", IntegerType, nullable = true)
      .add("_corrupt_record", StringType, nullable = true)

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
