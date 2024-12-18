package spark.scala.org.Amount

import org.apache.log4j._
import org.apache.spark._
import spark.scala.org.generic.InputOutputFileUtility

object Amount {
  System.setProperty("hadoop.home.dir", "C:\\winutils")

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    // Create a SparkContext using every core of the local machine
    val sc = new SparkContext("local[*]", "Amount")

    // reads data from customer-orders.csv file and computes the results
    val data = sc.textFile(InputOutputFileUtility.getInputPath("customer-orders.csv"))

    // Use our parseLines function to convert to (id, amount)
    val rdd = data.map(parseLine)

    //reduce by id as key and amount as value
    val total = rdd.reduceByKey((x, y) => x + y)

    //sort the fields on the basis of amount
    val sort = total.map(x => (x._2, x._1)).sortByKey()

    //swap it back to(id, amount)
    val swap = sort.map(item => item.swap)
    swap.saveAsTextFile(InputOutputFileUtility.getOutputPath("AmountOutput"))
  }

  //Convert input data to (customerID, amountSpent)
  def parseLine(line: String): (Int, Float) = {
    val fields = line.split(",")
    val id = fields(0).toInt
    val amount = fields(2).toFloat
    (id, amount)
  }
}
