package spark.scala.org.aadhaaranalysis

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import spark.scala.org.generic.InputOutputFileUtility

object AadhaarAnalysis {
  System.setProperty("hadoop.home.dir", "C:\\winutils")

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val sc = SparkSession.builder().master("local[*]").appName("AadhaarAnalysis").getOrCreate()
    import sc.implicits._

    /**
     * CHECKPOINT 1
     *  1. Load the data into Spark.
     */
    val readData = sc.read.format("csv").textFile(InputOutputFileUtility.getInputPath("aadhaar_data.csv"))
    val splitData = readData.map {
      line =>
        val col = line.split(",")
        Problem1(col(0), col(1), col(2), col(3), col(4), col(5), col(6), col(7), col(8), col(9), col(10), col(11), col(12))
    }.toDF()

    /**
     * 2.	View/result of the top 25 rows from each individual agency.
     */
    splitData.createOrReplaceTempView("problem1")
    sc.sql("select private_agency, count(private_agency) as topAgency from problem1 group by private_agency order by topAgency desc")
      .drop("topAgency").show(false)

    /**
     * CHECKPOINT 2
     * 1.	Describe the schema.
     */
    splitData.printSchema()

    /**
     * 2.	Find the count and names of registrars in the table.
     */
    splitData.groupBy('registrar).agg(count('registrar).as("count of registrar")).show(false)

    /**
     * 3.	Find the number of states, districts in each state and sub-districts in each district.
     */
    splitData.groupBy('state).agg(count('state).as("countOfState")).show(false)
    splitData.groupBy('district).agg(count('district).as("countOfDistrictInSate")).show(false)
    splitData.groupBy('sub_district).agg(count('sub_district).as("countOfSubDistrictInDistrict'")).show(false)

    /**
     * 4.	Find out the names of private agencies for each state
     */
    splitData.groupBy('state, 'private_agency).agg(count('state)).select('state, 'private_agency).orderBy(desc("state"),
      desc("private_agency")).show(20, truncate = false)

    /**
     * CHECKPOINT 3
     * 1.	Find top 3 states generating most number of Aadhaar cards?
     */
    splitData.select('state, 'aadhaar_generated).groupBy('state).agg(count('aadhaar_generated).as("countOfAdharCards"))
      .orderBy(desc("countOfAdharCards")).show(3, truncate = false)

    /**
     * 2.	Find top 3 districts where enrolment numbers are maximum?
     */
    splitData.select('district, 'aadhaar_generated).groupBy('district).agg(count('aadhaar_generated).as("countOfAdharCards"))
      .orderBy(desc("countOfAdharCards")).show(3, truncate = false)

    /**
     * 3.	Find the no. of Aadhaar cards generated in each state?
     */
    splitData.select('state, 'aadhaar_generated).groupBy('state).agg(count('aadhaar_generated).as("countOfAdharCards"))
      .orderBy(desc("countOfAdharCards")).show(false)

    /**
     * CHECKPOINT 4
     * 1.	Find the number of unique pin codes in the data?
     */
    splitData.select('pincode).agg(countDistinct('pincode)).show(false)

    /**
     * 2.	Find the number of Aadhaar registrations rejected in Uttar Pradesh and Maharashtra?
     */
    splitData.select('state, 'rejected).filter(splitData("state") === "Uttar Pradesh" || splitData("state") === "Maharashtra")
      .groupBy('state).agg(count('rejected)).show(false)

    /**
     * CHECKPOINT 5
     * 1.	Find the top 3 states where the percentage of Aadhaar cards being generated for males is the highest.
     */
    splitData.filter(splitData("gender") === "M").groupBy('state).agg(count('aadhaar_generated).as("countOfAdharCards"))
      .orderBy(desc("countOfAdharCards")).select('state).show(3, truncate = false)

    /**
     * 2.	Find in each of these 3 states, identify the top 3 districts where the percentage of Aadhaar cards being rejected for females is the highest.
     */
    splitData.filter(splitData("gender") === "M").groupBy('state).agg(count('aadhaar_generated).as("countOfAdharCards"))
      .orderBy(desc("countOfAdharCards")).select('state).limit(3).collect().foreach(x => {
        splitData.filter(splitData("state") === x.mkString("") && splitData("gender") === "F").groupBy('district)
          .agg(count('rejected).as("adharRejectForFemales")).orderBy(desc("adharRejectForFemales"))
          .select('district).limit(3).show(false)
      })

    /**
     * 3. Find the summary of the acceptance percentage of all the Aadhaar cards applications by bucketing the age group into 10 buckets.
     */
    splitData.filter(splitData("aadhaar_generated") =!= "0")
      .withColumn("bucket", pmod(hash($"age"), lit(10)))
      .repartition(10, $"bucket")
      .write.format("csv")
      .bucketBy(10, "age")
      .sortBy("age")
      .option("path", InputOutputFileUtility.getOutputPath("acceptancePercentageOfAllAadhaarCards"))
      .saveAsTable("Age")
  }

  private case class Problem1(data: String, registrar: String, private_agency: String, state: String, district: String, sub_district: String, pincode: String, gender: String,
                              age: String, aadhaar_generated: String, rejected: String, mobile_number: String, email_id: String)
}
