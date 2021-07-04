package spark.scala.org.Age

import spark.scala.org.InputOutputFileUtility
import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j._

object Age {
  System.setProperty("hadoop.home.dir", "C:\\winutils")

  //A function that splits a line of input into (age, numFriends)
  def parseLine(line: String) = {
    // Split by commas
    val fields = line.split(",")

    // Extract the age and numFriends fields, and convert to integers
    val age = fields(2).toInt
    val nofri = fields(3).toInt
    // Create a tuple that is our result.
    (age, nofri)
  }

  def main(args: Array[String]) {
    Logger.getLogger("org").setLevel(Level.ERROR)
    // Create a SparkContext using every core of the local machine
    val sc = new SparkContext("local[*]", "Age")

    // reads data from friends.csv file and computes the results
    val data = sc.textFile(InputOutputFileUtility.getInputPath("friends.csv"))

    // Use our parseLines function to convert to (age, numFriends)
    val rdd = data.map(parseLine)

    // We are starting with an RDD of form (age, numFriends) where age is the KEY and numFriends is the VALUE
    // We use mapValues to convert each numFriends value to a tuple of (numFriends, 1)
    // Then we use reduceByKey to sum up the total numFriends and total instances for each age, by
    // adding together all the numFriends values and 1's respectively.
    val totalByAge = rdd.mapValues(x => (x, 1)).reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2))

    // So now we have tuples of (age, (totalFriends, totalInstances))
    // To compute the average we divide totalFriends / totalInstances for each age.
    val averageByAge = totalByAge.mapValues(x => x._1 / x._2)

    //Collect the results from the RDD
    val results = averageByAge.collect()

    // Sort and print the final results.
    results.sorted.foreach(println)
  }
}
