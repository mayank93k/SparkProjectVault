package spark.scala.org.TravelAnalysis

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j._
import scala.math._
import org.apache.spark.rdd.RDD
import spark.scala.org.InputOutputFileUtility

//create class "TravelAnalysis1" 
class TravelAnalysis1(sc: SparkContext) {

  //create function destination to calculate the top 20 destination people travel the most
  def destination(t:String){
    
    //Input dataset in txt format
    val travel=sc.textFile(InputOutputFileUtility.getInputPath(t))
    
    //******************
    //Part1: Top 20 destination people travel the most
    //******************
    
    val split=travel.map(lines=>lines.split("\t")) //Splitting the txt file using "\t" which is tab delimited 
    val count=split.map(x=>(x(2),1)).reduceByKey(_+_)
    val sort=count.map(item=>item.swap).sortByKey(false)
    val top=sort.take(20)
    for(result<-top){
     val dest=result._2
     val num=result._1
     println(s"$dest,$num")
    }
  }
  
  //create function location to calculate top 20 location from people travel the most
  def location(t:String){
    
    //Input dataset in txt format
    val travel=sc.textFile(InputOutputFileUtility.getInputPath(t))
    
    //**********************
    //Part2: Top 20 location from people travel the most
    //**********************
    
    val split=travel.map(lines=>lines.split("\t")) //Splitting the txt file using "\t" which is tab delimited
    val count=split.map(x=>(x(1),1)).reduceByKey(_+_)
    val sort=count.map(item=>item.swap).sortByKey(false)
    val top=sort.top(20)
    for(result<-top){
      val location=result._2
      val num=result._1
      println(s"$location,$num")
    }
  }
  
  //create function revenue to calculate the cities that generate high airline revenues for travel
  def revenue(t:String){
    
    //Input dataset in txt format
    val travel=sc.textFile(InputOutputFileUtility.getInputPath(t))
    
    //**********************
    //Part3 cities that generate high airline revenues for travel
    //**********************
    
    val split=travel.map(lines=>lines.split("\t")) //Splitting the txt file using "\t" which is tab delimited
    val filter=split.filter(x=>{if(x(3).matches("1"))true else false})
    val count=filter.map(x=>(x(2),1)).reduceByKey(_+_)
    val sort=count.map(item=>item.swap).sortByKey(false)
    val top=sort.top(20)
    for(result<-top){
      val location=result._2
      val revenues=result._1
      println(s"$location,$revenues")
    }
  }
  
  //create function adults to calculate the no of adults traveling is greater than or equal to 2 and less than equal to 4
  def adults(t:String){
    
    //Input dataset in txt format
    val travel=sc.textFile(InputOutputFileUtility.getInputPath(t))
    
    //**************************
    //Part4 No of adults traveling is greater than or equal to 2 and less than equal to 4 
    //**************************
    
    val split=travel.map(lines=>lines.split("\t")) //Splitting the txt file using "\t" which is tab delimited
    val filter=split.filter(x=>{if(x(4) >= "2" && x(4) <= "4")true else false})
    val count=filter.map(x=>(x(4),1)).reduceByKey(_+_)
       count.foreach(println)
  }
}

object TravelAnalysis1{
  System.setProperty("hadoop.home.dir", "C:\\winutils")
  def main(args:Array[String]) {
    Logger.getLogger("org").setLevel(Level.ERROR)
    //Creation of SparkContext object
    val sc=new SparkContext("local[*]","TravelAnalysis")
    
    //object creation for class TravelAnalysis1
    val job=new TravelAnalysis1(sc)
    val results=job.destination("TravelData.txt")
    val results1=job.location("TravelData.txt")
    val results2=job.revenue("TravelData.txt")
    val results3=job.adults("TravelData.txt")
      }
}
