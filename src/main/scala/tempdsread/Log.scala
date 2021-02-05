package tempdsread

import java.util.logging.{Level, Logger}

object Log {
  def INFO(format: String, objects: Object*): Unit = {
    LOGGER.info(String.format(format, objects))
  }

  def WARNING(format: String, objects: Object*): Unit = {
    LOGGER.log(Level.WARNING, String.format(format, objects))
  }

  def ERROR(format: String, objects: Object*): Unit = {
    LOGGER.log(Level.SEVERE, String.format(format, objects))
  }

  val LOGGER = Logger.getLogger("main")
}
