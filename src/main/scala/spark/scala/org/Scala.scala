package spark.scala.org

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

import java.text.SimpleDateFormat

object Scala {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val conf = new SparkConf().setAppName("").setMaster("local").set("spark.executor.memory", "1g").set("spark.driver.allowMultipleContexts", "true")
    val sparkContext = new SparkContext(conf)

    val textRdd = sparkContext.textFile(InputOutputFileUtility.getInputPath("emp_data.csv"))
    val header = textRdd.first()
    val empRdd = textRdd.filter(data => data != header).map {
      line =>
        val col = line.split(",")
        Employee(col(0).toInt, col(1), col(2), col(3), col(4), col(5).toInt, col(6).toInt, col(7))
    }

    //Display the average salary of all the employee.
    println("Displaying the average salary")
    val avgSal = empRdd.map(emp => emp.sal).reduce(_ + _)
    println(s"Average salary is:${avgSal / empRdd.map(data => data.toString != header).count()}")

    //Display the minimum and max salary of clerk, salesman and manager.
    println("Displaying the min and max salary of clerk,salesman and manager")
    val maxSal = empRdd.filter(d => d.designation == "CLERK").map(sal => sal.sal)
    println(s"max sal of clerk:${maxSal.max()}")
    println(s"min sal of clerk:${maxSal.min()}")

    val maxSalS = empRdd.filter(d => d.designation == "SALESMAN").map(sal => sal.sal)
    println(s"max sal of salesman:${maxSalS.max()}")
    println(s"min sal of salesman:${maxSalS.min()}")

    val maxSalM = empRdd.filter(d => d.designation == "MANAGER").map(sal => sal.sal)
    println(s"max sal of manager:${maxSalM.max()}")
    println(s"min sal of manager:${maxSalM.min()}")

    //Display the record of those employees who belongs to IT and has a machine as a Desktop.
    println("Display the record of those employees who belongs to IT and has a machine as a Desktop.")
    empRdd.filter(p => p.machine == "Desktop" && p.designation == "IT").foreach(println)

    //Display the record of those employee whose name is start with 'A' ends with 'S'.
    val name = empRdd.filter(x => x.ename.toUpperCase.startsWith("A") && x.ename.toUpperCase.endsWith("S"))
    println("Display the record of those employee whose name is start with 'A' ends with 'S':")
    for (name1 <- name) {
      println(name1)
    }

    //Display the record of those employee who got hired before 12/30/1981
    println("Display the record of those employee who got hired before 12-30-1981")
    val date1 = "12-30-1981"
    val format = new SimpleDateFormat("MM-dd-yyyy")
    empRdd.filter(a => format.parse(a.hire_date).before(format.parse(date1))).foreach(println)

    //Display the record of those employee group by deptno.
    println("Display the record of all employee, group by deptno.")
    empRdd.map(x => x).groupBy(a => a.deptno).foreach(println)


    //Display the records who has designation as a Manager.
    println("Display the records who has designation as a Manager.")
    empRdd.filter(x => x.designation == "MANAGER").foreach(println)


    //Second highest salary and second lowest salary.
    println("The information of those employee who has second highest salary is:")
    val emp = empRdd.map(x => x.sal)
    val max_salary = emp.sortBy(x => x, ascending = false, 1)
    val second_highest_salary = max_salary.zipWithIndex().filter(index => index._2 == 1).map(_._1)
    second_highest_salary.foreach(println)


    println("The information of those employee who has second lowest salary is:")
    val min_salary = emp.sortBy(x => x, ascending = true, 1)
    val second_lowest_salary = min_salary.zipWithIndex().filter(index => index._2 == 1).map(_._1)
    second_lowest_salary.foreach(println)
  }

  case class Employee(empno: Int, ename: String, designation: String, manager: String, hire_date: String, sal: Int, deptno: Int, machine: String)

}
