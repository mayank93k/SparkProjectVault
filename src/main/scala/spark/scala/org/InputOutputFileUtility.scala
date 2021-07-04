package spark.scala.org

object InputOutputFileUtility {
  def getCurrentPath: String = "file:///" + System.getProperty("user.dir")

  def getDataDirPath: String = getCurrentPath + "/Data_Source/Input_Source/"

  def getOutputDirPath: String = getCurrentPath + "/Data_Source/Output_Source/"

  def getInputPath(fileName: String): String = getDataDirPath + fileName

  def getOutputPath(fileName: String): String = getOutputDirPath + fileName

  //  def getGeneratedFilePath() = getOutputDirPath + "/jsonOutput/*.json"
}
