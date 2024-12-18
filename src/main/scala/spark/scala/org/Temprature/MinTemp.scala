package spark.scala.org.Temprature

import org.apache.log4j._
import org.apache.spark._
import spark.scala.org.generic.InputOutputFileUtility

import scala.math.min

object MinTemp {
  System.setProperty("hadoop.home.dir", "C:\\winutils")

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    // Create a SparkContext using every core of the local machine
    val sc = new SparkContext("local[*]", "MinTemp")
    //val sc=new SparkContext("local[*]", "MaxTemp")
    //val sc=new SparkContext("local[*]", "MaxPrcp")

    // Read each line of input data
    val lines = sc.textFile(InputOutputFileUtility.getInputPath("temp.csv"))

    // Convert to (stationID, entryType, temperature) tuples
    val parsedLine = lines.map(parseLine)

    // Filter out all but TMIN entries
    val minTemp = parsedLine.filter(x => x._2 == "TMIN")
    //val maxTemp=parsedLine.filter(x=>x._2 == "TMAX")
    //val maxPrcp=parsedLine.filter(x=>x._2 == "PRCP")

    // Convert to (stationID, temperature)
    val stationTemps = minTemp.map(x => (x._1, x._3))
    //val stationTemps= maxTemp.map(x => (x._1 ,x._3.toFloat))
    //val stationTemps= maxPrcp.map(x => (x._1 ,x._3.toInt))

    // Reduce by stationID retaining the minimum temperature found
    val minTempByStation = stationTemps.reduceByKey((x, y) => min(x, y))
    //val maxTempByStation=stationTemps.reduceByKey((x,y)=>max(x,y))
    //val maxPrcpByStation=stationTemps.reduceByKey((x,y)=>max(x,y))


    // Collect, format, and print the results
    val results = minTempByStation.collect()
    //val results=maxTempByStation.collect()
    //val results=maxPrcpByStation.collect()

    for (result <- results.sorted) {
      val station = result._1
      val tempp = result._2
      val formatedTemp = f"$tempp%.2f F"
      println(s"$station minimum temprature: $formatedTemp")
      //println(s"$station maximum temprature: $formatedTemp")
      //println(s"$station maximum Prcp: $formatedTemp")
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
