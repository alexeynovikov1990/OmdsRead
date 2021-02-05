package tempdsread

import fswalker.{Directory}
import scala.collection.mutable.ArrayBuffer

class AddressProcessor(dir:Directory, addr:String) {
  private val root = dir
  private val address = addr

  def processAddress() : ArrayBuffer[String] = {
    val tokens = splitAddress()
    val paths = new ArrayBuffer[String]()
    processAddress(root, tokens, 0, paths)
    paths
  }

  def splitAddress() : ArrayBuffer[String] = {
    val tokens = new ArrayBuffer[String]()
    var addr = ""
    if (address.endsWith("**"))
      addr = address.substring(0, address.size - 1 - "**".size)
    else{
      if (address.endsWith("**/"))
        addr = address.substring(0, address.size - 1 - "**/".size)
      else
        addr = address
    }
    val parts = addr.split("/")
    for (p <- parts){
        if (p == "**"){
          if (tokens.last != "**")
            tokens.append(p)
        }
        else tokens.append(p)
      }
    tokens
  }

  def notToken(token:String) : Boolean = token == "**"
  def undefinedToken(token:String) : Boolean = token.endsWith("=*")
  def matchToken(token:String, str:String) : Boolean = {
    if (undefinedToken(token)){
      str.startsWith(token.substring(0, token.size - 1 - 1))
    }
    else (str == token)
  }

  def processAddress(dir:Directory, tokens:ArrayBuffer[String], tokenIdx:Int, paths:ArrayBuffer[String]) : Unit = {
    val curToken = tokens(tokenIdx)
    val subdirs = dir.getSubdirectories()
    if (notToken(curToken)){
      val iter = subdirs.iterator
      while (iter.hasNext){
        val p = iter.next()
        if (p._2.isDefined) {
          //проверяем имя директории на равенство следующему токену
          val nextToken = tokens(tokenIdx+1)
          if (matchToken(nextToken, p._1)){
                //Найдена последняя часть пути
                if (tokenIdx+1 == tokens.size - 1)
                  paths.append(p._2.get.getDirPath())
                else
                  processAddress(p._2.get, tokens, tokenIdx + 1, paths)
          }
          else processAddress(p._2.get, tokens, tokenIdx, paths)
        }
      }
    }
    else{
        val dirs = new ArrayBuffer[Directory]()
        val iter = subdirs.iterator
        while (iter.hasNext) {
          val p = iter.next()
          if (matchToken(curToken, p._1)) {
            if (p._2.isDefined)
              dirs.append(p._2.get)
          }
        }

        if (tokenIdx == tokens.size-1){//Поиск окончен
          for (d <- dirs){
            paths.append(d.getDirPath())
          }
        }
        else{
          for (d <- dirs){
            processAddress(d, tokens, tokenIdx + 1, paths)
          }
        }
    }
  }

}
