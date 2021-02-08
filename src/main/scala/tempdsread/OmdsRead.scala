package tempdsread

import org.apache.spark.sql.{Column, DataFrame}
import ot.dispatcher.sdk.{PluginCommand, PluginUtils}
import ot.dispatcher.sdk.core.SimpleQuery
import org.apache.spark.sql.functions.{col, collect_list, concat_ws, max, typedLit}

import scala.collection.immutable.Seq
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.HashMap
import fswalker._
import fastwalker.FastDirEnumerator

class OmdsRead(query: SimpleQuery, utils: PluginUtils) extends PluginCommand(query, utils, Set("from", "to")) {
  private var tdsPath:String = ""
  private var actualTime:Boolean = false
  private var address:String = ""
  private var field:String = ""
  private var operation:String = ""
  private var value:String = ""
  private val metrics:ArrayBuffer[String] = new ArrayBuffer[String]()
  private final val leafLenght = 8

  parseArguments(query.args)

  def transform(_df: DataFrame): DataFrame = {
    var result = _df
    val input = readData(tdsPath, address)
    val aggInput = aggByName(input, "_time")
    if (aggInput != Spark.spark.emptyDataFrame){
      if (!field.isEmpty && !operation.isEmpty && !value.isEmpty){
        var filteredFrame = filterData(aggInput, field, operation, value)
        if (filteredFrame != Spark.spark.emptyDataFrame){
          if (actualTime)
            filteredFrame = updateTime(filteredFrame)
          result = addPartitionsColumns(filteredFrame.select("_time", metrics: _*))
        }
        else
          println("Data matching filter parameters not found.")
      }
      else
        println("One or more filtering parameters are undefined.")
    }
    else
      println("Data not found. Check storage path and key.")

    result
  }

  def parseArguments(args:String) : Unit = {
    val tds = getKeyword("ds")
    if (tds.isDefined) {
      val param = tds.get
      if (param.compareTo("tds") == 0)
        tdsPath = pluginConfig.getString("data.tds")
      else{
        if (param.compareTo("fds") == 0)
          tdsPath = pluginConfig.getString("data.fds")
      }
    }
    val at = getKeyword("actual_time")
    if (at.isDefined){
      actualTime = (at.get.compareTo("true") == 0)
    }
    val addr = getKeyword("address")
    if (addr.isDefined)
      address = addr.get.replaceAll("#", "=")
    val fd = getKeyword("field")
    if (fd.isDefined)
      field = fd.get
    val op = getKeyword("op")
    if (op.isDefined)
      operation = op.get
    val v = getKeyword("value")
    if (v.isDefined)
      value = v.get
    val ms = getKeyword("metrics")
    if (ms.isDefined){
      metrics.appendAll(ms.get.split("#"))
      metrics += field
    }
  }

  def readData(tds:String, addr:String) : DataFrame = {

    val root = FastDirEnumerator.walkTree(tds)
    val addrproc= new AddressProcessor(root, addr)
    val paths = addrproc.processAddress()
    val leafs = checkLeafs(root, paths)
    val dr = new DataReader()
    for (p <- leafs){
      dr.addSchema(p)
    }
    dr.readAll()
  }

  def checkLeafs(root: Directory, paths:ArrayBuffer[String]) : ArrayBuffer[String] = {
    val result = new ArrayBuffer[String]()
    val rootPath = root.getDirPath()
    for (p <- paths){
      val relPath = p.substring(rootPath.size)
      val len = relPath.split("/").size
      if (len == leafLenght)
        result += p
        else{
        val dir = root.getDirectoryByPath(relPath)
        val leafs = new ArrayBuffer[String]()
        FastDirEnumerator.enumLeafsRecursivelly(dir.get, leafs)
        result ++= leafs
      }
    }
    result
  }

  def aggByName(df:DataFrame, name:String) : DataFrame = {
    val columns = new ArrayBuffer[String]()
    val colNames = df.columns
    for (n <- colNames){
      if (!n.equals(name))
        columns += n
    }
    //Агрегирующий sql-запрос
    val aggQuery = new StringBuilder()
    aggQuery.append("select _time, ")
    for (name <- columns){
      aggQuery.append("concat_ws(' ', collect_list(")
      aggQuery.append(name)
      aggQuery.append(")) as ")
      aggQuery.append(name)
      aggQuery.append(",")
    }
    aggQuery.deleteCharAt(aggQuery.size - 1)
    aggQuery.append(" from data group by _time")
    df.registerTempTable("data")
    df.sqlContext.sql(aggQuery.toString())
  }

  def filterData(frame:DataFrame, field:String, op:String, value:String) : DataFrame = {
    val operation = op.replaceAll("#", "=")
    val sb:StringBuilder = new StringBuilder()
    val sqlExp = sb.append(field).append(" ").append(operation).append(" ").append(value).toString
    frame.filter(sqlExp)
  }

  def updateTime(frame:DataFrame) : DataFrame = {
    val maxTimeTable = frame.agg(max("_time")).select("max(_time)")
    val maxTime = maxTimeTable.collectAsList().get(0).get(0).toString
    val rowCount = frame.count()
    val timeColumn:ArrayBuffer[String] = collection.mutable.ArrayBuffer.fill(rowCount.toInt)(maxTime)
    val timeColumnSeq = Seq[String](timeColumn: _*)
    frame.drop("_time")
    frame.withColumn("_time", typedLit(timeColumnSeq))
  }

  def addPartitionsColumns(frame:DataFrame) : DataFrame = {
    val partColumns = parseAddress()
    var result = frame
    val iter = partColumns.iterator
    while (iter.hasNext){
      val pair = iter.next()
      result = result.withColumn(pair._1, typedLit(pair._2))
    }
    result
  }

  def parseAddress() : HashMap[String,String] = {
    val kv = new mutable.HashMap[String,String]()
    val pairs = address.split("/")
    for (s <- pairs){
      val pair = s.split("=")
      if (pair.size == 2 && pair(1) != "*")
        kv.put(pair(0), pair(1))
    }
    kv
  }

}
