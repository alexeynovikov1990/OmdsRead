package fswalker

import java.nio.file.Files
import java.nio.file.Paths
import scala.collection.mutable.ArrayBuffer

object TestUtils {
    def createDirectory(path:String) : Unit = {
      Files.createDirectories(Paths.get(path))
    }
    def createFiles(path:String, filesCount:Int) : Unit = {
      for (i <- 1 to filesCount){
        val filePath = path + i.toString + ".txt"
        Files.write(Paths.get(filePath), "fswalker test".getBytes())
      }
    }
    def createDirectoryTree(rootPath:String, dirFilesCount:Int, levelDirsCount:Int, levelCount:Int) : Unit = {
      var totalFilesCount:Int = 0
      var totalDirsCount:Int = 0
      val paths:ArrayBuffer[String] = new ArrayBuffer[String]()
      val newPaths:ArrayBuffer[String] = new ArrayBuffer[String]()
      createDirectory(rootPath)
      paths += rootPath
      for (i <- 1 to levelCount){
        for (j <- 0 until paths.size){
          val curDir = paths(j)
          for (k <- 1 to levelDirsCount){
            val newDir = curDir + k.toString + "/"
            createDirectory(newDir)
            totalDirsCount += 1
            createFiles(newDir, dirFilesCount)
            totalFilesCount += dirFilesCount
            newPaths += newDir
          }
        }
        paths.clear()
        paths.appendAll(newPaths)
        newPaths.clear()
      }
      totalFilesCount += 1 //"RESULT.txt"
      val sb:StringBuilder = new StringBuilder()
      sb.append("Total files count = ")
      sb.append(totalFilesCount)
      sb.append(", total directories count = ")
      sb.append(totalDirsCount)
      sb.append(", all = ")
      sb.append(totalFilesCount + totalDirsCount)
      val result = sb.toString()
      println(result)
      val resultPath = rootPath + "RESULT.txt"
      Files.write(Paths.get(resultPath), result.getBytes())
    }
}
