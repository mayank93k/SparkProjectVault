package spark.scala.org.utilities

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.lit

/**
 * An object to read and process binary data files using a Cobol copybook in Apache Spark.
 */
object ReadBinFile {
  /**
   * The main method which initializes the Spark session, reads a binary file using a copybook,
   * and performs a simple transformation on the resulting DataFrame.
   *
   * @param args Command-line arguments (not used in this example).
   */
  def main(args: Array[String]): Unit = {
    // Initialize Spark session with an application name and local master setting
    val spark = SparkSession.builder().appName("BIN Reader").master("local").getOrCreate()

    // Define paths to the binary file and the corresponding Cobol copybook
    val binaryFilePath = "src/main/resources/binfile/samasaple_data.bin"
    val copybookPath = "src/main/resources/copybook/sample_data.cpy"

    /**
     * Reads the binary data file using the Cobrix library with specified options.
     * - Format: Specifies the data source format as "cobol".
     * - Copybook: The path to the Cobol copybook file used for data parsing.
     * - is_record_sequence: A boolean option indicating if the file has a record sequence number.
     */
    val dataFrame = spark.read
      .format("cobol")
      .option("copybook", copybookPath)
      .option("is_record_sequence", "false")
      .load(binaryFilePath)

    // Add a new column named "new" with a constant value of 1 to the DataFrame
    val enrichedDataframe = dataFrame.withColumn("new", lit(1))

    // Show the DataFrame contents
    enrichedDataframe.show()

    spark.stop()
  }
}
