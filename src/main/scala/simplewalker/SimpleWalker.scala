package simplewalker

import java.io.File


object SimpleWalker {

  def walkDirectory(path:String, dir: SimpleDirectory) : Unit = {
    val dirFile = new File(path)
    val files = dirFile.list()
    if (files != null){
      for (f <- files){
        var filePath = path + f
        val file = new File(filePath)
        if (file.isDirectory){
          filePath += "/"
          val subdir = new SimpleDirectory(filePath)
          dir.subdirs.put(f, Some(subdir))
          walkDirectory(filePath, subdir)
        }
        else{
          dir.fileNames += f
        }
      }
    }
  }

  def printTree(dir: SimpleDirectory) : Unit = {
    println(dir.getDirPath())
    val iter = dir.subdirs.iterator
    while (iter.hasNext)
      printTree(iter.next()._2.get)
  }

}