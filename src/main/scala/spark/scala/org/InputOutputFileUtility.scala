package spark.scala.org

object InputOutputFileUtility {
  def getInputPath(fileName: String): String = getDataDirPath + fileName

  def getDataDirPath: String = getCurrentPath + "/Data_Source/Input_Source/"

  def getOutputPath(fileName: String): String = getOutputDirPath + fileName

  def getOutputDirPath: String = getCurrentPath + "/Data_Source/Output_Source/"

  def getCurrentPath: String = "file:///" + System.getProperty("user.dir")

  //  def getGeneratedFilePath() = getOutputDirPath + "/jsonOutput/*.json"
}
