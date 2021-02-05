package fswalker

import scala.collection.mutable.HashMap
import scala.collection.mutable.HashSet
import scala.collection.Set
import scala.collection.mutable.ArrayBuffer

class Directory(val dirPath: String,
                private val enumerator: FileEnumerator[Directory]) {

  def enumerateEntries(): Boolean = enumerator.EnumerateEntries(dirPath, subdirs, fileNames)
  def getDirectoryByName(dirName: String): Option[Directory] = {
    val v:Option[Option[Directory]] = subdirs.get(dirName)
    if (v.isDefined) v.get
    else None
  }

  def getDirectoryByPath(relativePath: String): Option[Directory] = {
    val names: Array[String] = relativePath.split("/")
    var curDir: Option[Directory] = Option(this)
    var cnt: Int = 0
    while (cnt < names.length && curDir.isDefined){
      curDir = curDir.get.getDirectoryByName(names(cnt))
      cnt += 1
    }
    curDir
  }

  def hasRegularFiles(): Boolean = !fileNames.isEmpty
  def getDirPath(): String = dirPath

  override def equals(obj: Any): Boolean =
    if (obj == null) false
    else if (obj == this) true
    else {
      if (obj.getClass != this.getClass) false
      else {
        val dir: Directory = obj.asInstanceOf[Directory]
        dir.getDirPath == dirPath
      }
    }

  override def hashCode(): Int = dirPath.hashCode

  def allocDirectoryStructures(): ArrayBuffer[Directory] = {
    val dirnames: Set[String] = subdirs.keySet
    val dirs: ArrayBuffer[Directory] = new ArrayBuffer[Directory](subdirs.size)
    val iter:Iterator[String] = dirnames.iterator;
    var s:String = "";
    while (iter.hasNext) {
      s = iter.next()
      var sb: StringBuilder = new StringBuilder(dirPath)
      sb.append(s).append("/")
      val newdir: Directory = new Directory(sb.toString, enumerator)
      dirs += newdir
      subdirs.put(s, Option(newdir))
    }
    dirs
  }

  def copyFileNames(): HashSet[String] = {
    val cp:HashSet[String] = new HashSet[String]();
    cp ++ fileNames
    cp
  }
  def getFileNames(): HashSet[String] = fileNames
  def getSubdirectories() : HashMap[String, Option[Directory]] = subdirs

  private val subdirs: HashMap[String, Option[Directory]] = new HashMap()
  val fileNames: HashSet[String] = new HashSet()

}
