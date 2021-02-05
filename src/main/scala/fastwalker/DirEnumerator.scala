package fastwalker


import fswalker.Directory

import scala.collection.mutable.ArrayBuffer

abstract class DirEnumerator {
    def walkTree(root:String) : Directory
    def enumLeafs(root:Directory) : ArrayBuffer[String]
}
