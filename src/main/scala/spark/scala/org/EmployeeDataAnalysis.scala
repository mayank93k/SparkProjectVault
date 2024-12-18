package spark.scala.org

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DoubleType, IntegerType}
import spark.scala.org.common.logger.Logging

object EmployeeDataAnalysis extends Logging {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession.builder().appName("Employee Data Analysis").master("local").getOrCreate()

    val readCsvFile = spark.read.option("header", "true").csv("src/main/resources/input/xmlfile/csvfile/emp_data.csv")

    readCsvFile.show(false)
    readCsvFile.printSchema()

    /**
     * Display the average salary of all the employee.
     */
    logger.info("Displaying the average salary")
    val averageSalary = readCsvFile.withColumn("sal", col("sal").cast(DoubleType))
      .agg(round(avg(col("sal")), 2).as("AverageSalary"))

    averageSalary.show(false)

    /**
     * Display the minimum and max salary of clerk, director and software engineer.
     */
    logger.info("Displaying the max salary of clerk")
    val maxClerkSalary = readCsvFile.withColumn("sal", col("sal").cast(DoubleType))
      .filter(col("designation") === "Clerk")
      .agg(round(max(col("sal")), 2).as("ClerkMaxSalary"))
    maxClerkSalary.show(false)

    logger.info("Displaying the max salary of director")
    val maxDirectorSalary = readCsvFile.withColumn("sal", col("sal").cast(DoubleType))
      .filter(col("designation") === "Director")
      .agg(round(max(col("sal")), 2).as("DirectorMaxSalary"))
    maxDirectorSalary.show(false)

    logger.info("Displaying the max salary of software engineer")
    val maxSoftwareEngineerSalary = readCsvFile.withColumn("sal", col("sal").cast(DoubleType))
      .filter(col("designation") === "Software Engineer")
      .agg(round(max(col("sal")), 2).as("SoftwareEngineerMaxSalary"))
    maxSoftwareEngineerSalary.show(false)

    logger.info("Display the record of those employees who belongs to IT and has a machine as a Desktop")
    val filterITEmployee = readCsvFile.filter(col("machine") === "Desktop" && col("designation") === "IT")
    filterITEmployee.show(false)

    logger.info("Display the record of those employee whose name is start with 'A' ends with 'S'")
    val employeeNameStartWithAAndEndWithS = readCsvFile.filter(col("ename").startsWith("A") &&
      upper(col("ename")).endsWith("S"))
    employeeNameStartWithAAndEndWithS.show(false)

    logger.info("Display the record of those employee who got hired before 12/30/1981")
    val date = "12-30-1981"
    val filterHireDate = readCsvFile.filter(col("hire_date") < lit(date))
    filterHireDate.show(false)

    logger.info("Display the record of those employee group by deptno")
    val groupEmployees = readCsvFile.groupBy(col("deptno")).agg(count(col("deptno")) as "cnt")
      .filter(col("cnt") > "1")
    groupEmployees.show(false)

    logger.info("Display the records who has designation as a Manager")
    val displayManagerData = readCsvFile.filter(col("designation") === "Manager")
    displayManagerData.show(false)

    logger.info("Second highest salary and second lowest salary")
    logger.info("The information of those employee who has second highest salary is:")
    val highWindow = Window.orderBy(desc("sal"))
    val secondHighestSalary = readCsvFile.withColumn("sal", col("sal").cast(IntegerType))
      .withColumn("index", row_number().over(highWindow))
      .orderBy(asc("index"))
      .filter(col("index") === "2")
      .drop("index")
    secondHighestSalary.show(false)

    logger.info("The information of those employee who has second lowest salary is:")
    val lowWindow = Window.orderBy(asc("sal"))
    val secondLowestSalary = readCsvFile.withColumn("sal", col("sal").cast(IntegerType))
      .withColumn("index", row_number().over(lowWindow))
      .orderBy(asc("index"))
      .filter(col("index") === "2")
      .drop("index")
    secondLowestSalary.show(false)
  }
}
