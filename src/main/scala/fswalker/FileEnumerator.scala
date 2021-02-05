package fswalker

import scala.collection.mutable.HashMap
import scala.collection.mutable.HashSet

object FileEnumerator {

  object EnumType extends Enumeration {

    val GETDENTS: Type = new Type()
    val READDIR: Type = new Type()
    //val DIRSTREAM: Type = new Type()

    class Type extends Val
    implicit def convertValue(v: Value): Type = v.asInstanceOf[Type]

  }

}

abstract class FileEnumerator[T] {

  def EnumerateEntries(dirPath: String,
                       subdirectories: HashMap[String, Option[T]],
                       fileNames: HashSet[String]): Boolean

}
