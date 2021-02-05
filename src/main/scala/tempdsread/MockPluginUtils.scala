package tempdsread


import com.typesafe.config.{Config, ConfigFactory}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SparkSession}
import ot.dispatcher.sdk.PluginUtils

import java.nio.file.Paths

class MockPluginUtils(sparkSession: SparkSession) extends PluginUtils{

  override def getLoggerFor(classname: String): Logger ={
    val log = Logger.getLogger(classname)
    log.setLevel(Level.DEBUG)
    log
  }

  override def logLevelOf(name: String): String = "DEBUG"

  override def printDfHeadToLog(log: Logger, id: Int, df: DataFrame): Unit = df.show

  override def sendError(id: Int, message: String): Nothing = throw new CommandTestException(message)

  override def spark: SparkSession = sparkSession

  override def executeQuery(query: String, df: DataFrame): DataFrame = throw new NotTestableException

  override def executeQuery(query: String, index: String, startTime: Int, finishTime: Int): DataFrame = throw new NotTestableException

  override def pluginConfig: Config = ConfigFactory.parseURL(Paths.get("resources/plugin.conf").toAbsolutePath.toUri.toURL)

  override def mainConfig: Config = ConfigFactory.parseURL(Paths.get("resources/application.conf").toAbsolutePath.toUri.toURL)
}

class CommandTestException(msg: String) extends Exception(msg)

class NotTestableException extends Exception("This functionality can't be tested from plugin tests")
