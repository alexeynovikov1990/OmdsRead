package fastwalker

import fswalker.{FSWalker, FileEnumerator, Directory}
import scala.collection.mutable.ArrayBuffer

object FastDirEnumerator extends DirEnumerator {

  def walkTree(root:String) : Directory = {
    val walker: FSWalker = new FSWalker(root, FileEnumerator.EnumType.GETDENTS)
    walker.start()
    walker.waitForTasksComplete(1000)
    walker.getRoot()
  }

  def enumLeafs(root:Directory) : ArrayBuffer[String] = {
    val paths = new ArrayBuffer[String]()
    enumLeafsRecursivelly(root, paths)
    paths
  }

  def enumLeafsRecursivelly(dir: Directory, paths:ArrayBuffer[String]) : Unit = {
    val subdirs = dir.getSubdirectories()
    if (subdirs.isEmpty) {//лист
      paths += dir.getDirPath()
      return
    } else{
      val iter = subdirs.iterator
      while(iter.hasNext){
        val dir = iter.next()._2
        if (dir.isDefined)
          enumLeafsRecursivelly(dir.get, paths)
      }
    }
  }
}
