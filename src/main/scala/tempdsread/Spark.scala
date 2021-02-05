package tempdsread

import org.apache.spark.sql.{DataFrame, SparkSession}

object Spark {
  val spark: SparkSession = SparkSession.builder()
    .master("local[*]")
    .appName("app")
    .getOrCreate()

  def SourceToDataFrame(srcPath: String): DataFrame = {
    spark.read.json(srcPath)
  }
}
