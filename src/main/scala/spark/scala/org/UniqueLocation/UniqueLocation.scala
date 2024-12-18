package spark.scala.org.UniqueLocation

import org.apache.log4j._
import org.apache.spark._
import org.apache.spark.rdd.RDD

import scala.collection.Map

class UniqueLocation(sc: SparkContext) {

  // read data from text files and computes the results. 
  def run(): RDD[(String, String)] = {
    val transations = sc.textFile("src/main/resources/input/uniquelocation/transactions_test.txt")
    val transPair = transations.map { t =>
      val p = t.split("\t")
      (p(2).toInt, p(1).toInt)
    }
    val users = sc.textFile("src/main/resources/input/uniquelocation/users_test.txt")
    val userPair = users.map { t =>
      val p = t.split("\t")
      (p(0).toInt, p(3))
    }
    val result = processData(transPair, userPair)
    sc.parallelize(result.toSeq).map(t => (t._1.toString, t._2.toString))
  }

  private def processData(t: RDD[(Int, Int)], u: RDD[(Int, String)]): Map[Int, Long] = {
    val jne = t.join(u).values.distinct
    jne.countByKey
  }
}

object UniqueLocation {

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val sc = new SparkContext("local[*]", "UniqueLocation")
    val job = new UniqueLocation(sc)
    val results = job.run()
    results.coalesce(1).saveAsTextFile("src/main/resources/input/uniquelocation/locationOut")
    sc.stop()
  }
}
  
