package tempdsread

import org.apache.spark.sql.{DataFrame}
import ot.dispatcher.sdk.{PluginCommand, PluginUtils}
import ot.dispatcher.sdk.core.SimpleQuery
import java.nio.file.Files
import java.nio.file.Paths
import fastwalker.FastDirEnumerator

class OmdsExtractSchema(query: SimpleQuery, utils: PluginUtils) extends PluginCommand(query, utils, Set("from", "to")) {
  private var tdsDir:String = ""

  parseArguments(query.args)

  def transform(_df: DataFrame): DataFrame = {
    val leafs = FastDirEnumerator.enumLeafs(FastDirEnumerator.walkTree(tdsDir))
    for (l <- leafs){
      extractSchemaForObject(l)
    }
    _df
  }

  def parseArguments(args:String) : Unit = {
    val tdsVal = getKeyword("tds")
    if (tdsVal.isDefined)
      tdsDir = tdsVal.get
  }

  def extractSchemaForObject(path:String) : Unit = {
    val df = Spark.spark.read.parquet(path)
    val sc = df.schema.toDDL
    val schemaPath = path + "_schema.ddl"
    Files.write(Paths.get(schemaPath), sc.getBytes())
  }

}
