package spark.scala.org.schemainfer

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.types._

import scala.reflect.runtime.universe._

object SchemaReader {
  val spark = SparkSession.builder().master("local").getOrCreate()
  val readFile = spark.read.schema(inferJsonSchema)

  /**
   * Utility to infer schema for different source system
   *
   * @return Struct Typesafe config
   */
  private def inferJsonSchema(): StructType = {
    readSchema[readInput]
  }

  /**
   * Utility to read schema from T type case class
   *
   * @tparam T : T type case class
   * @return
   */
  private def readSchema[T <: Product : TypeTag]: StructType = ScalaReflection.schemaFor[T].dataType.asInstanceOf[StructType]

  private case class readInput(name: String, age: Int)
}
