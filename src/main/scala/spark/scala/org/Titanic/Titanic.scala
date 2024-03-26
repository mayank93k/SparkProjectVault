package spark.scala.org.Titanic

import org.apache.log4j._
import org.apache.spark._
import spark.scala.org.InputOutputFileUtility

object Titanic {
  System.setProperty("hadoop.home.dir", "C:\\winutils")

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    //Creation of SparkContext object
    val sc = new SparkContext("local[*]", "Titanic") //local[*] : as much thread as possible considering your CPUs
    val data = sc.textFile(InputOutputFileUtility.getInputPath("titanic-data.txt")) //Input dataset in txt format

    //Steps to remove Header from txt file.
    val header = data.first() //selecting the first row of record which contains header
    val data1 = data.filter(row => row != header) //eliminating the header record, so newData have dataset without header

    //Splitting the txt file using "," which is Comma delimited
    val split = data1.map(fields => fields.split("\t"))

    //********************************
    //case1: If gender is "male" then display the name of that person
    //********************************

    //filter the gender as "male"
    val maledata = split.filter(x => x(4) == "male")

    //map name of each male
    val male = maledata.map(x => x(3)).collect()
    male.foreach(println) //display statement

    //********************************
    //case2: If gender is "male" and age is "22" then display name and age of that person
    //********************************

    //filter the gender as "male" and age as "22"
    val MaleData = split.filter(x => x(4) == "male" && x(5) == "22")

    //map name and age of each who satisfy the filter condition
    val Male = MaleData.map(x => (x(3), x(5))).collect()
    Male.foreach(println) //display statement

    //*********************************
    //case3: No of female and male traveling
    //*********************************

    //filter the gender as "female" or "male"
    val travelerData = split.filter(x => x(4) == "female" || x(4) == "male")

    //map the gender and reduce it calculate the no of male and female
    val noOfTraveler = travelerData.map(x => (x(4), 1)).reduceByKey(_ + _)
    noOfTraveler.foreach(println) //display statement

    //**********************************
    //case4: Passenger distribution on the basis of pclass
    //**********************************

    //filter the pclass as "1", "2" and "3"
    val pClass = split.filter(x => x(2) == "1" || x(2) == "2" || x(2) == "3")

    //map the pclass value and reduce it to count the no of passenger in pclass
    val classCount = pClass.map(x => (x(2), 1)).reduceByKey(_ + _)
    classCount.foreach(println) //display statement
  }
}