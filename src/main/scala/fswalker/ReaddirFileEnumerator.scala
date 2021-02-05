package fswalker

import scala.collection.mutable.HashMap
import scala.collection.mutable.HashSet
import scala.collection.mutable.ArrayBuffer

class ReaddirFileEnumerator[T] extends FileEnumerator[T] {

  def EnumerateEntries(dirPath: String,
                       subdirectories: HashMap[String, Option[T]],
                       fileNames: HashSet[String]): Boolean = {
    subdirectories.clear()
    fileNames.clear()
    val entries: ArrayBuffer[LowLevelStructures.LinuxDirent64] = LowLevelUtils.READDIR.ReadDirEntries(dirPath)
    var entry: LowLevelStructures.LinuxDirent64 = null
    for (idx <- 0 to (entries.length - 1)){
      entry = entries(idx);
      if (entry.getFileName.compareTo(".") != 0 && entry.getFileName.compareTo("..") != 0)
      {
        if (entry.isDirectory) subdirectories.put(entry.getFileName, None)
        else fileNames.add(entry.getFileName)
      }
    }
    (!subdirectories.isEmpty)
  }

}

