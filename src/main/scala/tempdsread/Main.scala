package tempdsread

import ot.dispatcher.sdk.core.SimpleQuery

import scala.collection.immutable.Seq
import fastwalker.FastDirEnumerator
import fswalker.SystemSettings
import fswalker._
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SaveMode}
import org.apache.spark.sql.functions.{collect_list, concat_ws}
import simplewalker._

import java.nio.file.Files
import java.nio.file.Paths

object Main {

  def leafEnumeratorTest() : Unit = {
    val path = "/home/test/data/_time_range=1572555600000-1572642000000/"
    val leafs = FastDirEnumerator.enumLeafs(FastDirEnumerator.walkTree(path))
    println(leafs)
  }

  def extractionSchemaTest() : Unit = {
    val schemacmd = new OmdsExtractSchema(SimpleQuery("tds=/home/test/smalldata/"), new MockPluginUtils(Spark.spark))
    schemacmd.transform(Spark.spark.emptyDataFrame)
  }

  def addressProcessorTest(dir:Directory, address:String) : Unit = {
    val proc = new AddressProcessor(dir, address)
    val paths = proc.processAddress()
    println(paths)
  }

  def simpleWalkerTest(path:String) : Unit = {
    val root = new SimpleDirectory(path)
    SimpleWalker.walkDirectory(path, root)
    SimpleWalker.printTree(root)
  }

  def createTestParquets() : Unit = {

    val data1 = Seq(
      Row(1,	"1993", "Alexey", "Alexeev", "1984"),
      Row(2,	"1995",	"Ivan", "Ivanov",	"1974"),
      Row(3,	"2001",	"Petr", "Petrov",		"1993")
    )

    val data2 = Seq(
      Row(4,	2011,	"Maxim",	"Maximov",	1978),
      Row(5,	1999,	"Vasya", "Pupkin",	1996)
    )

    val data3 = Seq(
      Row(6,  "Vova",	"Putin",  "-"),
      Row(7,   "Dimitry", "Medvedev", "-")
    )

    val data4 = Seq(
      Row(6, "dvorets"),
      Row(7, "utochka")
    )

    val data5 = Seq(
      Row(10, "pass")
    )

    //Схемы
    val schema1 = StructType( Array(
      StructField("id", IntegerType,true),
      StructField("registeryear", StringType, true),
      StructField("name", StringType, true),
      StructField("surname", StringType, true),
      StructField("bornyear", StringType, true)
    ))

    val schema2 = StructType( Array(
      StructField("id", IntegerType,true),
      StructField("registeryear", IntegerType, true),
      StructField("name", StringType, true),
      StructField("surname", StringType, true),
      StructField("bornyear", IntegerType, true)
    ))

    val schema3 = StructType( Array(
      StructField("id", IntegerType,true),
      StructField("name", StringType, true),
      StructField("surname", StringType, true),
      StructField("gender", StringType, true)
    ))

    val schema4 = StructType( Array(
      StructField("id", IntegerType,true),
      StructField("password", StringType, true)
    ))

    val df1 = Spark.spark.createDataFrame(Spark.spark.sparkContext.parallelize(data1), schema1)
    val df2 = Spark.spark.createDataFrame(Spark.spark.sparkContext.parallelize(data2), schema2)
    val df3 = Spark.spark.createDataFrame(Spark.spark.sparkContext.parallelize(data3), schema3)
    val df4 = Spark.spark.createDataFrame(Spark.spark.sparkContext.parallelize(data4), schema4)
    val df5 = Spark.spark.createDataFrame(Spark.spark.sparkContext.parallelize(data5), schema4)

    val path = "/home/test/Documents/5task/data/"
    val parquet1 = path + "data1/"
    val parquet2 = path + "data2/"
    val parquet3 = path + "data3/"
    val parquet4 = path + "data4/"
    val parquet5 = path + "data5/"
    df1.write.parquet(parquet1)
    df2.write.parquet(parquet2)
    df3.write.parquet(parquet3)
    df4.write.parquet(parquet4)
    df5.write.parquet(parquet5)

    val schemaPath1 = parquet1 + "_schema.ddl"
    val schemaPath2 = parquet2 + "_schema.ddl"
    val schemaPath3 = parquet3 + "_schema.ddl"
    val schemaPath4 = parquet4 + "_schema.ddl"
    val schemaPath5 = parquet5 + "_schema.ddl"
    Files.write(Paths.get(schemaPath1), df1.schema.toDDL.getBytes())
    Files.write(Paths.get(schemaPath2), df2.schema.toDDL.getBytes())
    Files.write(Paths.get(schemaPath3), df3.schema.toDDL.getBytes())
    Files.write(Paths.get(schemaPath4), df4.schema.toDDL.getBytes())
    Files.write(Paths.get(schemaPath5), df5.schema.toDDL.getBytes())
    df1.show()
    df2.show()
    df3.show()
    df4.show()
    df5.show()
  }

  def mergeAlgoTest() : Unit = {
    val obj1 = "/home/test/Documents/5task/data/data1/"
    val obj2 = "/home/test/Documents/5task/data/data2/"
    val obj3 = "/home/test/Documents/5task/data/data3/"
    val obj4 = "/home/test/Documents/5task/data/data4/"
    val obj5 = "/home/test/Documents/5task/data/data5/"

    val sp = new DataReader()
    sp.addSchema(obj1)
    sp.addSchema(obj2)
    sp.addSchema(obj3)
    sp.addSchema(obj4)
    sp.addSchema(obj5)
    sp.readAll().show()
  }

  def aggTest() : Unit = {
    val data1 = Seq(
      Row(1,	"1993", "", "", "1984"),
      Row(1,	"",	"Ivan", "",	""),
      Row(1,	"",	"", "Petrov",		"")
    )
    val schema1 = StructType( Array(
      StructField("id", IntegerType,true),
      StructField("registeryear", StringType, true),
      StructField("name", StringType, true),
      StructField("surname", StringType, true),
      StructField("bornyear", StringType, true)
    ))
    val df1 = Spark.spark.createDataFrame(Spark.spark.sparkContext.parallelize(data1), schema1)
    df1.groupBy("id").agg(
                                concat_ws(" ", collect_list("registeryear")) as "registeryear",
                                concat_ws(" ", collect_list("name")) as "name",
                                concat_ws(" ", collect_list("surname")) as "surname",
                                concat_ws(" ", collect_list("bornyear")) as "bornyear"
                              ).show()

    val data2 = Seq(
      Row(1,	"1993", "Alexey", "Alexeev", "1984"),
      Row(2,	"1995",	"Ivan", "Ivanov",	"1974"),
      Row(3,	"2001",	"Petr", "Petrov",		"1993")
    )
    val df2 = Spark.spark.createDataFrame(Spark.spark.sparkContext.parallelize(data2), schema1)
    df2.groupBy("id").agg(
      concat_ws(" ", collect_list("registeryear")) as "registeryear",
      concat_ws(" ", collect_list("name")) as "name",
      concat_ws(" ", collect_list("surname")) as "surname",
      concat_ws(" ", collect_list("bornyear")) as "bornyear"
    ).show()
  }

  def sqlGroupTest() : Unit = {
    val data1 = Seq(
      Row(1,	"1993", "", "", "1984"),
      Row(1,	"",	"Ivan", "",	""),
      Row(1,	"",	"", "Petrov",		"")
    )
    val schema1 = StructType( Array(
      StructField("id", IntegerType,true),
      StructField("registeryear", StringType, true),
      StructField("name", StringType, true),
      StructField("surname", StringType, true),
      StructField("bornyear", StringType, true)
    ))
    val df1 = Spark.spark.createDataFrame(Spark.spark.sparkContext.parallelize(data1), schema1)
    df1.registerTempTable("data")
    val sqlQuery = "select id,concat_ws(' ', collect_list(registeryear)) as registeryear, concat_ws(' ', collect_list(name)),concat_ws(' ', collect_list(surname)),concat_ws(' ', collect_list(bornyear)) from data group by id"
    df1.sqlContext.sql(sqlQuery).show()
  }

  def printParquetTest() : Unit = {
    val input = "/home/test/testdata/_time_range=1572555600000-1572642000000/_year=2019/_month=11/_day=01/_department_num=4/_deposit=tailakovskoe/_pad_num=1/_object=скважина_73/"
    val df = Spark.spark.read.parquet(input)
    df.show(100)
  }

  def main(args: Array[String]): Unit = {

    SystemSettings.Init()

    val omdsReadCmdline = "ds=tds actual_time=false address=_time_range#1572555600000-1572642000000/_year#2019/**/_department_num#4/ field=well_num op=# value=73 metrics=address#pad_num#deposit#_time_preprocess"
    val omdsread: OmdsRead = new OmdsRead(SimpleQuery(omdsReadCmdline), new MockPluginUtils(Spark.spark))
    val df = omdsread.transform(Spark.spark.emptyDataFrame)
    df.show()

    //sqlGroupTest()
    //aggTest()
    //val tds:String = "/home/test/smalldata/"
    //val addr1 = "_time_range=1572555600000-1572642000000/_year=2019/**/_department_num=4/"
    //val addr2 = "_time_range=1572555600000-1572642000000/_year=2019/_month=11/_day=01/_department_num=4/_deposit=tailakovskoe/_pad_num=1/_object=*/"
    //val addr3 = "_time_range=1572555600000-1572642000000/_year=2019/_month=11/_day=*/_department_num=4/_deposit=tailakovskoe/"
    //val addr4 = "_time_range=1572555600000-1572642000000/_year=2019/**/**/**/_deposit=tailakovskoe/"
    //val addr5 = "_time_range=1572555600000-1572642000000/_year=2019/_month=11/_day=01/_department_num=4/**"
    //val addr6 = "_time_range=1572555600000-1572642000000/_year=2019/_month=11/_day=01/_department_num=4/_deposit=tailakovskoe/_pad_num=*/_object=*/"
    //val root = walkDirectory(tds)
    //addressProcessorTest(root, addr1)
    //addressProcessorTest(root, addr2)
    //addressProcessorTest(root, addr3)
    //addressProcessorTest(root, addr4)
    //addressProcessorTest(root, addr5)
    //addressProcessorTest(root, addr6)

    //schemaProcessorTest()
    //createTestParquets()
    //schemaProcessorTestNew()
    //compareFields()
    //createTestParquets()
    //schemaProcessorTestNew()
    //mergeAlgoTest()
  }
}
