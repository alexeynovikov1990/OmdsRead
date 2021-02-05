package fswalker

import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.HashMap
import scala.collection.mutable.HashSet


class Getdents64Enumerator[T] extends FileEnumerator[T] {

  override def EnumerateEntries(dirPath: String,
                                subdirectories: HashMap[String, Option[T]],
                                fileNames: HashSet[String]): Boolean = {
    subdirectories.clear()
    fileNames.clear()
    val content: Option[Array[Byte]] = LowLevelUtils.GETDENTS64.readFilesInfo(dirPath)
    var entry: LowLevelStructures.LinuxDirent64 = null
    if (content.isDefined) {
      val entries: ArrayBuffer[LowLevelStructures.LinuxDirent64] = LowLevelUtils.GETDENTS64.ReadDirEntries(content.get)
      for (idx <- 0 to (entries.length - 1)){
        entry = entries(idx)
        if (entry.getFileName.compareTo(".") != 0 && entry.getFileName.compareTo("..") != 0)
        {
          if (entry.isDirectory) subdirectories.put(entry.getFileName, None)
          else fileNames.add(entry.getFileName)
        }
      }
      true
    } else false
  }

}