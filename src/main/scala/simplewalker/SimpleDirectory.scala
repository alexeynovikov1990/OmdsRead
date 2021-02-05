package simplewalker

import scala.collection.mutable.HashMap
import scala.collection.mutable.HashSet

class SimpleDirectory(dirPath: String) {

  val subdirs: HashMap[String, Option[SimpleDirectory]] = new HashMap()
  val fileNames: HashSet[String] = new HashSet()

  def getDirPath(): String = dirPath
}
