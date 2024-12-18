package spark.scala.org.travelanalysis

import org.apache.spark._
import spark.scala.org.common.logger.Logging

//create class "TravelAnalysis1"
class TravelAnalysis(sc: SparkContext) extends Logging {

  //create function destination to calculate the top 20 destination people travel the most
  def destination(): Unit = {

    //Input dataset in txt format
    val travel = sc.textFile("src/main/resources/input/travelanalysis/TravelData.txt")

    //******************
    // Part1: Top 20 destination people travel the most
    //******************
    logger.info("Part1: Top 20 destination people travel the most")
    val split = travel.map(lines => lines.split("\t")) //Splitting the txt file using "\t" which is tab delimited
    val count = split.map(x => (x(2), 1)).reduceByKey(_ + _)
    val sort = count.map(item => item.swap).sortByKey(ascending = false)
    val top = sort.take(20)
    for (result <- top) {
      val dest = result._2
      val num = result._1
      println(s"$dest,$num")
    }
  }

  //create function location to calculate top 20 location from people travel the most
  def location(): Unit = {

    //Input dataset in txt format
    val travel = sc.textFile("src/main/resources/input/travelanalysis/TravelData.txt")

    //**********************
    // Part2: Top 20 location from people travel the most
    //**********************
    logger.info("Part2: Top 20 location from people travel the most")
    val split = travel.map(lines => lines.split("\t")) //Splitting the txt file using "\t" which is tab delimited
    val count = split.map(x => (x(1), 1)).reduceByKey(_ + _)
    val sort = count.map(item => item.swap).sortByKey(ascending = false)
    val top = sort.top(20)
    for (result <- top) {
      val location = result._2
      val num = result._1
      println(s"$location,$num")
    }
  }

  //create function revenue to calculate the cities that generate high airline revenues for travel
  def revenue(): Unit = {

    //Input dataset in txt format
    val travel = sc.textFile("src/main/resources/input/travelanalysis/TravelData.txt")

    //**********************
    // Part3 cities that generate high airline revenues for travel
    //**********************
    logger.info("Part3 cities that generate high airline revenues for travel")
    val split = travel.map(lines => lines.split("\t")) //Splitting the txt file using "\t" which is tab delimited
    val filter = split.filter(x => {
      if (x(3).matches("1")) true else false
    })
    val count = filter.map(x => (x(2), 1)).reduceByKey(_ + _)
    val sort = count.map(item => item.swap).sortByKey(ascending = false)
    val top = sort.top(20)
    for (result <- top) {
      val location = result._2
      val revenues = result._1
      println(s"$location,$revenues")
    }
  }

  //create function adults to calculate the no of adults traveling is greater than or equal to 2 and less than equal to 4
  def adults(): Unit = {

    //Input dataset in txt format
    val travel = sc.textFile("src/main/resources/input/travelanalysis/TravelData.txt")

    //**************************
    // Part4 No of adults traveling is greater than or equal to 2 and less than equal to 4
    //**************************
    logger.info("Part4 No of adults traveling is greater than or equal to 2 and less than equal to 4")
    val split = travel.map(lines => lines.split("\t")) //Splitting the txt file using "\t" which is tab delimited
    val filter = split.filter(x => {
      if (x(4) >= "2" && x(4) <= "4") true else false
    })
    val count = filter.map(x => (x(4), 1)).reduceByKey(_ + _)
    count.foreach(println)
  }
}

object TravelAnalysis {
  def main(args: Array[String]): Unit = {
    //Creation of SparkContext object
    val sc = new SparkContext("local[*]", "travelanalysis")

    //object creation for class TravelAnalysis1
    val job = new TravelAnalysis(sc)
    job.destination()
    job.location()
    job.revenue()
    job.adults()
  }
}
