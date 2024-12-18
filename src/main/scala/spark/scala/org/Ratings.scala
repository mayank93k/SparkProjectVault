package spark.scala.org

import org.apache.log4j._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import spark.scala.org.common.logger.Logging

object Ratings extends Logging {
  def main(arr: Array[String]): Unit = {
    performDataFrameOperation()
  }

  private def performDataFrameOperation(): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession.builder().appName("Ratings").master("local").getOrCreate()

    //Creating a DataFrame from a Data Source
    val df = spark.read.format("csv").option("header", "true").load(InputOutputFileUtility.getInputPath("Ratings.csv"))

    logger.info("Printing Schema.")
    df.printSchema()

    logger.info("Display limited record.....[userId,movieId,rating,timestamp")
    df.select("userId", "movieId", "rating").take(20).toList.foreach(println)

    logger.info("Performing order by query on dataframe on ascending order...")
    df.select("userId", "movieId", "rating").orderBy("movieId").take(50).seq.foreach(println)

    logger.info("Performing order by query on dataframe on descending order...")
    df.select("userId", "movieId", "rating").sort(desc("userId")).take(10).seq.foreach(println)

    logger.info("Performing filter operation....when rating is greater than 3")
    df.select("userId", "movieId", "rating").filter(df("rating") > 3).take(10).toList.foreach(println)

    logger.info("Performing filter operation....when rating is 3 and movie Id is greate than 30")
    df.select("userId", "movieId", "rating").filter(df("rating") === 3 && df("movieId") >= 30).take(10).
      toList.foreach(println)

    logger.info("Performing groupby operation....")
    df.select("userId", "movieId", "rating").filter(df("movieId") <= 30).groupBy("rating").
      agg(countDistinct("rating") as "rating accroding to the group").take(10).toList.foreach(println)

    logger.info("Performing groupby operation....")
    df.select("userId", "movieId", "rating").filter(df("movieId") <= 30).groupBy("rating").
      agg(countDistinct("rating") as "rating accroding to the group").take(10).seq.foreach(println)

    val joindf1 = df.select("userId", "movieId", "rating").filter(df("movieId") <= 300).groupBy("rating").
      agg(countDistinct("rating") as "rating accroding to the group").toDF()

    val joindf2 = df.select("userId", "movieId", "rating").filter(df("movieId") >= 300).groupBy("rating").
      agg(countDistinct("rating") as "rating accroding to the group").toDF()

    val joinDF = joindf1.union(joindf2)
    logger.info("Perform join operaion:")
    joinDF.show(false)
  }
}
