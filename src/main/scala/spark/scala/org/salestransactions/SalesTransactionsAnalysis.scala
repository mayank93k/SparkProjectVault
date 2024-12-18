package spark.scala.org.salestransactions

import org.apache.spark._
import spark.scala.org.common.logger.Logging

object SalesTransactionsAnalysis extends Logging {

  def main(args: Array[String]): Unit = {
    //Creation of SparkContext object
    val sc = new SparkContext("local[*]", "SalesTransactionsAnalysis") //local[*] : as much thread as possible considering your CPUs
    val data = sc.textFile("src/main/resources/input/salestransactions/SalesJan2009.csv") //Input dataset in csv format

    //Steps to remove Header from csv file.
    val header = data.first() //selecting the first row of record which contains header
    val newData = data.filter(row => row != header) //eliminating the header record, so newData have dataset without header

    //Calling the parseLine function/method
    val rdd = newData.map(parseLine)

    //************************
    // Case1: display the details of customer which includes name along with their address(city,state,country)of
    // each customer who buy product 1 and use payment-type as "Mastercard".
    //************************
    logger.info("Case1: display the details of customer which includes name along with their address(city,state,country) " +
      "of each customer who buy product 1 and use payment-type as Mastercard.")
    //filter the product-type as "product1" and payment-type as "Mastercard"
    val product = rdd.filter(x => x._1 == "Product1" && x._3 == "Mastercard")

    //map the distinct "name" using distinct keyword and "address" as key value pair
    val details = product.map(x => (x._2.distinct, x._7))

    //use action to collect all the details
    val getDetails = details.collect()
    for (result <- getDetails) {
      val name = result._1
      val address = result._2
      logger.info(s"$name from $address buy Product1 and Payment-Type is Mastercard")
    }

    //************************
    // Case2: Generate the Payment_type used in Country "United States".
    //************************
    logger.info("Case2: Generate the Payment_type used in Country \"United States\".")
    //filter the Country as "United States".
    val country = rdd.filter(x => x._4 == "United States")

    //map the Payment-type as key and reduce it by using key.
    val paymentType = country.map(x => (x._3, 1)).reduceByKey(_ + _)

    //swap the key value pair to sort the key(as value is numeric so now after swap key is numeric)
    val swap = paymentType.map(line => line.swap).sortByKey(ascending = false)

    //use action to collect all the details
    val count = swap.collect()
    for (result <- count) {
      val NoofCards = result._1
      val PaymentType = result._2
      logger.info(s"$PaymentType => $NoofCards")
    }

    //**************************
    // Case3: Find the no of people lives in state "England".
    //**************************
    logger.info("Case3: Find the no of people lives in state \"England\".")
    //filter the State as "England".
    val state = rdd.filter(x => x._5 == "England")

    //use name to count the no of people
    val NoOfPeople = state.map(x => x._2).count()
    logger.info(s"No_of_People:$NoOfPeople")

    //*****************************
    // Case4:
    // Case4a) Get the Geography(Latitude and Longitude) of each customer who buy Product3 along with their name.
    //*****************************
    logger.info("Case4a) Get the Geography(Latitude and Longitude) of each customer who buy Product3 along with their name.")
    //filter the Product-type as "Product3".
    val Product = rdd.filter(x => x._1 == "Product3")

    //map the "name" and "geography" as key value pair
    val geography = Product.map(x => (x._2, x._6))

    //use action to collect all the details
    val Collect = geography.collect()
    for (result <- Collect) {
      val name = result._1
      val geography = result._2
      logger.info(s"Geography(Latitude and Longitude) of $name who buy Product3 => $geography")
    }

    //***********************************
    // Case4b) Count the No of customer who buy Product3
    //***********************************
    logger.info("Case4b) Count the No of customer who buy Product3")
    //use count action to count the number of customer
    val NoOfCustomer = geography.count
    logger.info(s"No of customer who buy Product3: $NoOfCustomer")
  }

  //create method to include the fields of the input file
  def parseLine(line: String): (String, String, String, String, String, String, String) = {
    val fields = line.split(",")
    //Splitting the csv file using ",",which is Comma delimited
    val product = fields(1)
    val name = fields(4)
    val paymentType = fields(3)
    val country = fields(7)
    val state = fields(6)

    // In Geography variable we are including the values of Latitude and Longitude as a single value.
    val Geography = (fields(10), fields(11)).toString

    // In Address variable we are including the values of City,State and Country as a single String "Address".
    val Address = (fields(5), fields(6), fields(7)).toString
    (product, name, paymentType, country, state, Geography, Address)
  }
}