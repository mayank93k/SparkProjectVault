package spark.scala.org.generic

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions.{desc, lit, sum}
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}

import scala.util.Try

object Patient {
  Logger.getLogger("org").setLevel(Level.ERROR)

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("Patient").master("local").getOrCreate()

    import spark.implicits._

    //Creating a DataFrame from an RDD
    val rowsRDD = spark.sparkContext.textFile(InputOutputFileUtility.getInputPath("inpatientCharges.csv"))
    val RDD = rowsRDD.map { row => row.split(",") }
      .map { x => Patient(x(0), Try(x(1).toInt).toOption, x(2), x(3), x(4), x(5), Try(x(6).toInt).toOption, x(7), Try(x(8).toInt).toOption, Try(x(9).toDouble).toOption, Try(x(10).toDouble).toOption, Try(x(11).toDouble).toOption) }
    val df = RDD.toDF()

    df.show()
    df.printSchema()
    val schema_old = StructType(StructField("b1", StringType, nullable = true) :: StructField("b2", StringType, nullable = true) :: Nil)
    /*    val data = sc.parallelize(Seq(Row(" ", " ")))
        val testDataFrame_old = sqlContext.createDataFrame(data, schema_old) */

    /* testDataFrame_old.printSchema()
     testDataFrame_old.show(false)*/
    val a = "test_b1"
    val b = "test_b2"
    val c = "'test_a2"

    val schema = StructType(StructField("a1", schema_old, nullable = true) :: StructField("a2", StringType, nullable = true) :: Nil)

    val data1 = spark.sparkContext.parallelize(Seq(Row(Row(a, b), c)))

    val testDataFrame_new = spark.createDataFrame(data1, schema)

    testDataFrame_new.printSchema()
    testDataFrame_new.show(false)

    val dataa = testDataFrame_new.withColumn("b1", lit("test_b1")).withColumn("b2", lit("test_b2'")).withColumn("a2'", lit("test_a2"))
    dataa.show(false)
    testDataFrame_new.write.format("org.apache.spark.sql.json").save(InputOutputFileUtility.getOutputPath("jsonOutput_test2"))

    println(df.groupBy("ProviderState").avg("AverageTotalPayments").orderBy("ProviderState").count())

    //Problem Statement 1: Find the amount of Average Covered Charges per state.

    df.groupBy("ProviderState").avg("AverageCoveredCharges").show

    //Problem Statement 2: Find the amount of Average Total Payments charges per state.

    df.groupBy("ProviderState").avg("AverageTotalPayments").show

    //Problem Statement 3: Find the amount of Average Medicare Payments charges per state.

    df.groupBy("ProviderState").avg("AverageMedicarePayments").show

    //Problem Statement 4: Find out the total number of Discharges per state and for each disease.

    df.groupBy("ProviderState", "DRGDefinition").sum("TotalDischarges").show
    df.groupBy("ProviderState", "DRGDefinition").sum("TotalDischarges").
      orderBy(desc(sum("TotalDischarges").toString)).show

    df.createOrReplaceTempView("patient")
    val countDF = spark.sql("SELECT count(*) AS cnt FROM patient")
    countDF.show()
  }

  private case class Patient(DRGDefinition: String, ProviderId: Option[Int], ProviderName: String, providerStreetAddress: String, ProviderCity: String, ProviderState: String, ProviderZipCode: Option[Int], HospitalReferralRegionDescription: String,
                             TotalDischarges: Option[Int], AverageCoveredCharges: Option[Double], AverageTotalPayments: Option[Double], AverageMedicarePayments: Option[Double])
}
