package spark.scala.org.wordcount

import org.apache.log4j._
import org.apache.spark._
import org.apache.spark.sql.SparkSession
import spark.scala.org.InputOutputFileUtility

object WordCount {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    //Creation of SparkContext object
    val spark = SparkSession.builder.appName("wordcount").master("local").getOrCreate()

    // reads each line of data from book.txt file and computes the results
    val lines = spark.read.textFile("src/main/resources/input/textfile/book.txt").rdd

    // Split using a regular expression that extracts words
    val p = lines.flatMap(line => line.split("\\W+"))

    // Normalize everything to lowercase
    val q = p.map(x => x.toLowerCase())

    // Count of the occurrences of each word
    val rr = q.map(x => (x, 1)).reduceByKey((x, y) => x + y)

    // Flip (word, count) tuples to (count, word) and then sort by key (to counts on the basis of key)
    val count = rr.map(x => (x._2, x._1)).sortByKey()

    //Swap it back to (word, count)
    val swap = count.map(item => item.swap)

    //store the output to file
    swap.saveAsTextFile("src/main/resources/input/textfile/WordCountOutput")
  }
}
