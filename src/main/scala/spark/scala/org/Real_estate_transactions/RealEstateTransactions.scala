package spark.scala.org.Real_estate_transactions

import org.apache.log4j._
import org.apache.spark._
import spark.scala.org.InputOutputFileUtility

object RealEstateTransactions {
  System.setProperty("hadoop.home.dir", "C:\\winutils")

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    //Creation of SparkContext object
    val sc = new SparkContext("local[*]", "Protein") //local[*] : as much thread as possible considering your CPUs
    val data = sc.textFile(InputOutputFileUtility.getInputPath("Sacramentorealestatetransactions.csv")) //Input dataset in csv format

    //Steps to remove Header from csv file.
    val header = data.first() //selecting the first row of record which contains header
    val data1 = data.filter(row => row != header) //eliminating the header record, so newData have dataset without header

    //Calling the parseLine function/method
    val rdd = data1.map(parseLine)

    //*****************************
    //Case1: Calculate price of each flat and address who is having 4 "beds" and 2 "baths" and display the result if Price of flat is
    // greater than or equal to 300000 and less than or equal to 500000.
    //*****************************

    //filter the no of beds and baths respectively
    val Filter = rdd.filter(x => x._1 == 4 && x._2 == 2)

    //map the price and address as key value pair
    val calculate = Filter.map(x => (x._6, x._7))

    //use action to collect all the details
    val Details = calculate.collect()
    for (result <- Details) {
      val Address = result._2
      val Price = result._1
      if (Price >= 300000 && Price <= 500000) {
        println(s"Price of flat is: $Price and Address is: $Address")
      }
    }

    //******************************
    //Case2: If flat is having no of "beds" is 3 and Residence_type is "Condo" then display sale_date along with its area in sq__ft.
    //******************************

    //filter the no of beds and Res_type respectively
    val Filter_data = rdd.filter(x => x._1 == 3 && x._4 == "Condo")

    //filter the Area in sq__ft (eliminate all the zeros)
    val eliminate = Filter_data.filter(x => x._3 != 0)

    //map the sale_date and area as key value pair
    val details = eliminate.map(x => (x._5, x._3))

    //use action to collect all the details
    val getDetails = details.collect()
    for (result <- getDetails) {
      val Sale_Date = result._1
      val Area = result._2
      println(s"$Sale_Date and $Area in sq__ft")
    }
  }

  //create method to include the fields of the input file
  def parseLine(line: String): (Int, Int, Int, String, String, Int, String) = {
    val fields = line.split(",") //Splitting the csv file using ",",which is Comma delimited
    val beds = fields(4).toInt
    val baths = fields(5).toInt
    val area = fields(6).toInt
    val Res_type = fields(7)
    val sale_date = fields(8)
    val price = fields(9).toInt

    // In Address variable we are including the values of Street,City,State and Zip as a single String "address".
    val address = (fields(0), fields(1), fields(3), fields(2).toInt).toString
    (beds, baths, area, Res_type, sale_date, price, address)
  }
}