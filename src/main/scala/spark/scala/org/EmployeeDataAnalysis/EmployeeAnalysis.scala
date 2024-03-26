package spark.scala.org.EmployeeDataAnalysis

class EmployeeAnalysis {

  def empty(): Any = {
    val emp = new Empl
    val list = emp.populateRecord()
    println(s"List is empty or not: ${list.isEmpty}")
  }


  //Display the average salary of all the employee.
  def getAvg: Any = {
    val emp = new Empl
    val list = emp.populateRecord()
    val average = list.map(li => li.Salary).sum
    val Avg = average / list.size
    println(s"Average salary of all the employee is: $Avg")
  }


  //Display the minimum and max salary of IT, HR and Finance.
  def getMinMax: Any = {
    val emp = new Empl
    val list = emp.populateRecord()
    val MaxIT = list.filter(p => p.Department == "IT").map(li => li.Salary)
    println(s"Max Salary of IT Department is: ${MaxIT.max}")
    println(s"Min Salary of IT Department is: ${MaxIT.min}")

    val MaxHr = list.filter(p => p.Department == "Hr").map(li => li.Salary)
    println(s"Max Salary of Hr Department is: ${MaxHr.max}")
    println(s"Min Salary of Hr Department is: ${MaxHr.min}")

    val MaxFi = list.filter(p => p.Department == "Finance").map(li => li.Salary)
    println(s"Max Salary of Finance Department is: ${MaxFi.max}")
    println(s"Min Salary of Finance Department is: ${MaxFi.min}")
  }


  //Display the record of those employees who belongs to IT and has a machine as a Desktop.
  def getDesk: Any = {
    val emp = new Empl
    val list = emp.populateRecord()
    val desktop = list.filter(p => p.Machine == "Desktop" && p.Department == "IT")
    println("The record of those employees who belongs to IT and has a machine as a Desktop is:")
    for (desk <- desktop) {
      println(desk)
    }
  }


  //Display the record of those employee whose name is start with 'A' ends with 'S'.
  def getName: Any = {
    val emp = new Empl
    val list = emp.populateRecord()
    val name = list.filter(x => x.Name.toUpperCase.startsWith("A") && x.Name.toUpperCase.endsWith("S"))
    println("Display the record of those employee whose name is start with 'A' ends with 'S':")
    for (name1 <- name) {
      println(name1)
    }
  }


  //Display the record of those employee who don't have a phone no.
  def getPhone: Any = {
    val emp = new Empl
    val list = emp.populateRecord()
    val Phone = list.filter(p => p.phoneNo == " ")
    println("Display the record of those employee who don't have a phone no:")
    for (phone <- Phone) {
      println(phone)
    }
  }


  //Display the record of those employee whose phone number start 91 and 11.
  def getPhoneNo: Any = {
    val emp = new Empl
    val list = emp.populateRecord()
    val Phone = list.filter(x => x.phoneNo.startsWith("91") || x.phoneNo.startsWith("11"))
    println("Display the record of those employee whose phone number start 91 and 11:")
    for (phone <- Phone) {
      println(phone)
    }
  }


  //Display the designation, count and city who has designation as a SSE.
  def getSSE: scala.collection.mutable.Map[String, Int] = {
    val eemp = scala.collection.mutable.Map[String, Int]()
    val emp = new Empl
    val list = emp.populateRecord()
    list.foreach(emp1 => {
      if (emp1.Designation.equals("SSE")) {
        if (eemp.contains(emp1.City)) {
          var count = eemp(emp1.City)
          count += 1
          eemp += emp1.City -> count
        } else {
          eemp += emp1.City -> 1
        }
      }
    })
    for (a <- eemp) {
      println(s"The Designation is: SSE, City and Count is: $a")
    }
    eemp
  }


  //Display the information of those employee who has second highest salary and second lowest salary.
  def getSalary: Any = {
    val emp = new Empl
    emp.populateRecord()
  }


  //Update the existing record of two employee from actual value to dummy value like String as a "Transferred." and numeric as a 0000.
  def getRecord: scala.collection.mutable.MutableList[Employee] = {
    val rec = scala.collection.mutable.MutableList[Employee]()
    val emp1 = new Empl
    val list = emp1.populateRecord()
    println("The existing record of two employee with dummy value:")
    list.foreach(emp2 => {
      if ((null != emp2.Name && emp2.Name.nonEmpty) && (emp2.Name == "mayank" || emp2.Name == "sid")) {
        val emp3 = emp2.copy(Designation = "Transferred", Salary = 0000000.0)
        rec += emp3
        for (record <- rec) {
          println(record)
        }
      }
    })
    rec
  }


  //Display the average salary of each department.
  def getAvgSal: Any = {
    val emp = new Empl
    val list = emp.populateRecord()
    val a = list.count(x => x.Department == "IT")
    val avgSal1 = list.filter(x => x.Department == "IT").map(li => li.Salary).sum
    val Avg1 = avgSal1 / a
    println(s"Average salary of all the employee in IT Department is: $Avg1")
    val b = list.count(x => x.Department == "Hr")
    val avgSal2 = list.filter(x => x.Department == "Hr").map(li => li.Salary).sum
    val Avg2 = avgSal2 / b
    println(s"Average salary of all the employee in HR Department is: $Avg2")
    val c = list.count(x => x.Department == "Finance")
    val avgSal3 = list.filter(x => x.Department == "Finance").map(li => li.Salary).sum
    val Avg3 = avgSal3 / c
    println(s"Average salary of all the employee in Finance Department is: $Avg3")
  }
}

object EmployeeAnalysis {
  def main(args: Array[String]): Unit = {
    val b = new EmployeeAnalysis
    b.empty()
    b.getAvg
    b.getMinMax
    b.getDesk
    b.getName
    b.getPhone
    b.getPhoneNo
    b.getSSE
    b.getSalary
    b.getRecord
    b.getAvgSal
  }
}