package fswalker

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.HashSet

class FSWalker(rootDirPath: String, t: FileEnumerator.EnumType.Type) {

  var enumerator: FileEnumerator[Directory] = _
  if (t == FileEnumerator.EnumType.GETDENTS) enumerator = new Getdents64Enumerator()
  else if (t == FileEnumerator.EnumType.READDIR) enumerator = new ReaddirFileEnumerator()

  def start(): Unit = {
    threadPool.start()
    threadPool.addTask(new WalkTask(rootDirectory))
  }

  def isTasksCompleted(): Boolean = threadPool.isTasksCompleted

  def waitForTasksComplete(attemptCount: Int): Boolean = {
    var rslt:Boolean = false
    var cnt:Int = 0
    while (cnt < attemptCount && !isTasksCompleted) {
      Thread.sleep(CONSTANTS.completionTimeout)
      cnt += 1
    }
    (cnt != attemptCount)
  }

  def getRoot(): Directory = rootDirectory

  private class WalkTask(directory: Directory) extends Runnable {

    override def run(): Unit = {
      try {
        var curDir: Option[Directory] = Option(directory)
        while (curDir.isDefined && curDir.get.enumerateEntries()) {
          val subdirs: ArrayBuffer[Directory] = curDir.get.allocDirectoryStructures()
          if (!subdirs.isEmpty) {
            for (i <- 0 until subdirs.size - 1) {
              threadPool.addTask(new WalkTask(subdirs(i)))
            }
            curDir = Option(subdirs(subdirs.size - 1))
          } else curDir = None
        }
      } catch {
        case ex: InterruptedException => ex.printStackTrace()

      }
    }
  }

  def getAllFiles() : HashSet[String] = {
    val files:HashSet[String] = new mutable.HashSet[String]()
    getDirectoryFiles(files, rootDirectory)
    files
  }

  def getDirectoryFiles(files:HashSet[String], dir:Directory) : Unit = {
      var path:String = dir.dirPath
      for (e <- dir.fileNames){
        val p:StringBuilder = new StringBuilder(path)
        p ++= e
        files.add(p.toString())
      }
      for (d <- dir.getSubdirectories()){
        if (d._2.isDefined)
          getDirectoryFiles(files, d._2.get)
      }
  }

  private val rootDirectory: Directory = new Directory(rootDirPath, enumerator)

  private val threadPool: FSThreadPool = new FSThreadPool(
    SystemSettings.cpu.getCoresCount - 1)

}
