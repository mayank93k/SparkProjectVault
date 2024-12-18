package spark.scala.org.temperature

import org.apache.spark._
import spark.scala.org.common.logger.Logging

import scala.math.{max, min}

object MinTemp extends Logging {
  def main(args: Array[String]): Unit = {
    // Create a SparkContext using every core of the local machine
    val sc = new SparkContext("local[*]", "MinTemp")
    //val sc=new SparkContext("local[*]", "MaxTemp")
    //val sc=new SparkContext("local[*]", "MaxPrcp")

    // Read each line of input data
    val lines = sc.textFile("src/main/resources/input/tempretaure/temp.csv")

    // Convert to (stationID, entryType, temperature) tuples
    val parsedLine = lines.map(parseLine)

    // Filter out all but TMIN entries
    val minTemp = parsedLine.filter(x => x._2 == "TMIN")
    val maxTemp = parsedLine.filter(x => x._2 == "TMAX")
    val maxPrcp = parsedLine.filter(x => x._2 == "PRCP")

    // Convert to (stationID, temperature)
    val minStationTemps = minTemp.map(x => (x._1, x._3))
    val maxStationTemps = maxTemp.map(x => (x._1, x._3))
    val maxStationPrcp = maxPrcp.map(x => (x._1, x._3.toInt))

    // Reduce by stationID retaining the minimum temperature found
    val minTempByStation = minStationTemps.reduceByKey((x, y) => min(x, y))
    val maxTempByStation = maxStationTemps.reduceByKey((x, y) => max(x, y))
    val maxPrcpByStation = maxStationPrcp.reduceByKey((x, y) => max(x, y))


    // Collect, format, and print the results
    val minTempResults = minTempByStation.collect()
    val maxTempResults = maxTempByStation.collect()
    val maxPrcpResults = maxPrcpByStation.collect()

    for (result <- minTempResults.sorted) {
      val station = result._1
      val temp = result._2
      val formattedTemp = f"$temp%.2f F"
      logger.info(s"$station minimum temperature: $formattedTemp")
    }
    for (result <- maxTempResults.sorted) {
      val station = result._1
      val temp = result._2
      val formattedTemp = f"$temp%.2f F"
      logger.info(s"$station maximum temperature: $formattedTemp")
    }
    for (result <- maxPrcpResults.sorted) {
      val station = result._1
      val temp = result._2
      val formattedTemp = f"$temp%.2f F"
      logger.info(s"$station maximum prcp: $formattedTemp")
    }
  }

  //create method to include the fields of the input file
  def parseLine(line: String): (String, String, Float) = {
    val fields = line.split(",") //Splitting the csv file using ",",which is Comma delimited
    val stationID = fields(0)
    val entryType = fields(2)
    val temp = fields(3).toFloat * 0.1f * (9.0f / 5.0f) + 32.0f
    //val precp=fields(3)
    (stationID, entryType, temp)
    //(stationID, entryType, precp)
  }
}
