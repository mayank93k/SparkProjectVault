package spark.scala.org.IPLMatchesAnalysis

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j._
import org.apache.spark.rdd.PairRDDFunctions
import spark.scala.org.InputOutputFileUtility

object IplAnalysis {
  System.setProperty("hadoop.home.dir", "C:\\winutils")

  def main(args: Array[String]) {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val sc = new SparkContext("local[*]", "IPL")
    val data = sc.textFile(InputOutputFileUtility.getInputPath("iplmatch.csv"))
    val split = data.map(line => line.split(",")) //loading the data from csv file

    //filtering the bad records if any,the total number of columns are 19 if any record having less than 19 columns are filtered out.
    val filtering_bad_records = split.filter(x => x.length < 19)

    //Case1.Which stadium is best suitable for first batting

    //we are extracting the columns that are required for our analysis
    val extracting_columns = filtering_bad_records.map(x => (x(7), x(11), x(12), x(14)))

    //we are filtering the won_by_run column having more than 0 runs
    val bat_first_won = extracting_columns.filter(x => x._2 != "0")

    //we are preparing a key-value pair with the Venue column and a numeric 1 has been added to it so as to count the number of
    //first_bat_wons in that stadium
    val number = bat_first_won.map(x => (x._4, 1)).reduceByKey(_ + _)
    val Swap = number.map(item => item.swap).sortByKey(false).collect
    Swap.foreach(println) //printing of result

    //Case2.No of matches that each stadium has been venued

    val total_matches_per_venue = filtering_bad_records.map(x => (x(14), 1)).reduceByKey(_ + _)
    val Swap1 = total_matches_per_venue.map(item => item.swap).sortByKey(false)
    Swap1.foreach(println) //printing of result

    //Case3.The winning percentage of each stadium for first_bat_won

    //we have joined the two RDD’s, bat_first_won and total_matches_per_venue
    //percentage of first_bat_won venues by dividing the number of matches won by batting first and the total number of matches in that venue.
    //   val join = Swap.join(Swap1).map(x=>(x._1,(x._2._1*100/x._2._2)))
    //   val JSwap=join.map(item => item.swap).sortByKey(false).collect
    //     JSwap.foreach(println) //printing of result

    //Case4.Which stadium is best suitable for first Bowling

    //we will take out the columns toss_decision, won_by_runs, won_by_wickets, venue.
    val extracting_columns1 = filtering_bad_records.map(x => (x(7), x(11), x(12), x(14)))

    //the columns which are having won_by_wickets value as 0
    val bowl_first_won = extracting_columns1.filter(x => x._3 != "0").map(x => (x._4, 1)).reduceByKey(_ + _)
    val Swap2 = bowl_first_won.map(item => item.swap).sortByKey(false).collect
    Swap2.foreach(println) //printing of result

    //Case5.The total of number of matches each stadium has venued(or we can use case 2)

    val total_matches_per_venue1 = filtering_bad_records.map(x => (x(14), 1)).reduceByKey(_ + _)
    val Swap3 = total_matches_per_venue1.map(item => item.swap).sortByKey(false).collect
    Swap3.foreach(println) //printing of result

    //Case6.The winning percentage of each stadium for bowl_first_won

    //perform join operation on the total number of matches in that venue and bowl_first_won
    //   val join1 = Swap2.join(Swap3).map(x=>(x._1,(x._2._1*100/x._2._2)))
    //   val Jswap=join1.map(item => item.swap).sortByKey(false).collect
    //     Jswap.foreach(println) //printing of result
  }
}