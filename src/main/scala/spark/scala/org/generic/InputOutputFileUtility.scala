package spark.scala.org.generic

object InputOutputFileUtility {
  def getInputPath(fileName: String): String = getDataDirPath + fileName

  private def getDataDirPath: String = getCurrentPath + "/Data_Source/Input_Source/"

  def getOutputPath(fileName: String): String = getOutputDirPath + fileName

  private def getOutputDirPath: String = getCurrentPath + "/Data_Source/Output_Source/"

  private def getCurrentPath: String = "file:///" + System.getProperty("user.dir")
}
