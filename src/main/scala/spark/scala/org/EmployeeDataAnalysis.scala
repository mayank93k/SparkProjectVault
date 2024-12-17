package spark.scala.org

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.DoubleType
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
      .filter(col("designation")==="Clerk")
      .agg(round(max(col("sal")), 2).as("ClerkMaxSalary"))
    maxClerkSalary.show(false)

    logger.info("Displaying the max salary of director")
    val maxDirectorSalary = readCsvFile.withColumn("sal", col("sal").cast(DoubleType))
      .filter(col("designation")==="Director")
      .agg(round(max(col("sal")), 2).as("DirectorMaxSalary"))
    maxDirectorSalary.show(false)

    logger.info("Displaying the max salary of software engineer")
    val maxSoftwareEngineerSalary = readCsvFile.withColumn("sal", col("sal").cast(DoubleType))
      .filter(col("designation")==="Software Engineer")
      .agg(round(max(col("sal")), 2).as("SoftwareEngineerMaxSalary"))
    maxSoftwareEngineerSalary.show(false)


    //    //Display the record of those employees who belongs to IT and has a machine as a Desktop.
    //    println("Display the record of those employees who belongs to IT and has a machine as a Desktop.")
    //    empRdd.filter(p => p.machine == "Desktop" && p.designation == "IT").foreach(println)
    //
    //    //Display the record of those employee whose name is start with 'A' ends with 'S'.
    //    val name = empRdd.filter(x => x.ename.toUpperCase.startsWith("A") && x.ename.toUpperCase.endsWith("S"))
    //    println("Display the record of those employee whose name is start with 'A' ends with 'S':")
    //    for (name1 <- name) {
    //      println(name1)
    //    }
    //
    //    //Display the record of those employee who got hired before 12/30/1981
    //    println("Display the record of those employee who got hired before 12-30-1981")
    //    val date1 = "12-30-1981"
    //    val format = new SimpleDateFormat("MM-dd-yyyy")
    //    empRdd.filter(a => format.parse(a.hire_date).before(format.parse(date1))).foreach(println)
    //
    //    //Display the record of those employee group by deptno.
    //    println("Display the record of all employee, group by deptno.")
    //    empRdd.map(x => x).groupBy(a => a.deptno).foreach(println)
    //
    //
    //    //Display the records who has designation as a Manager.
    //    println("Display the records who has designation as a Manager.")
    //    empRdd.filter(x => x.designation == "MANAGER").foreach(println)
    //
    //
    //    //Second highest salary and second lowest salary.
    //    println("The information of those employee who has second highest salary is:")
    //    val emp = empRdd.map(x => x.sal)
    //    val max_salary = emp.sortBy(x => x, ascending = false, 1)
    //    val second_highest_salary = max_salary.zipWithIndex().filter(index => index._2 == 1).map(_._1)
    //    second_highest_salary.foreach(println)
    //
    //
    //    println("The information of those employee who has second lowest salary is:")
    //    val min_salary = emp.sortBy(x => x, ascending = true, 1)
    //    val second_lowest_salary = min_salary.zipWithIndex().filter(index => index._2 == 1).map(_._1)
    //    second_lowest_salary.foreach(println)
    //  }
    //
    //  case class Employee(empno: Int, ename: String, designation: String, manager: String, hire_date: String, sal: Int, deptno: Int, machine: String)

  }
}
