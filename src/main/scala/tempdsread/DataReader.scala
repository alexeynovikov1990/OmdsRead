package tempdsread

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.types.{StructField, StructType}

import scala.collection.mutable
import scala.collection.mutable.{ArrayBuffer, HashMap, HashSet}
import java.nio.file.Files
import java.nio.file.Paths

/*
Алгоритм:
Вход: отображение objid - набор полей объекта
Выход: 2 набора полей для каждого objid. В одном находится список полей, которые нужно переименовать, в другом - которые нужно добавить.

Под операциями суммы и пересечения наборов понимаются операции над этими наборами как над множествами.
1.Найти пересечение всех наборов полей.
2.Найти сумму всех наборов полей как множество ключей отображения (fields), сопоставляющего конкретное поле набору id объектов, его содержащих.
3.Если разность между найденными суммой и пересечением есть пустое множество, то схемы идентичны и данные зачитываются без каких либо изменений.
4.Если же схемы не идентичны, то
	1.путем обхода полученной суммы множеств получаем отображение, сопоставляющее имени поля набор полей с этим именем из всех таблиц.(список конфликтов)
	2.Далее для каждого objid и его схемы выполняется следующий алгоритм:
		1.Находится разность между суммой наборов полей и набором полей, соответствующим определенному objid.
		2.Выполняется поиск каждого уникального имени поля из этой разности в списке конфликтов.
			1.Если этому имени поля соответствует одно единственное поле, то конфликтов нет
			и в этом случае поле попадает в список предназначенных для добавления в таблицу.
			2.Если же в списке конфликтов этому имени поля соответствует более одного поля, то конфликт имеет место.
			В этом случае
				1.для каждого поля в списке
					1.определяется (при помощи fields) набор объектов, его содержащих.
					2.если objid из этого набора равен objid обрабатываемого объекта, то соответствующее поле переименовывается.
					3.если же objid указывает на другой объект, то в набор добавляется новое поле соответствующего типа, имя которого
					получается добавлением строки _[objid.hashCode] к имени поля.

Сложность решения "влоб" N(N-1)/2*N*N ~ O(N^4), сложность приведенного алгоритма O(N^2) в обмен на необходимость считать хэши.
 */

class DataReader {

  def addSchema(objId:String) : Unit = {
    val schemaPath = objId + "_schema.ddl"
    val schemaDDL = Files.readAllBytes(Paths.get(schemaPath)).map(_.toChar).mkString
    val schema = StructType.fromDDL(schemaDDL)
    val fldSet = new mutable.HashSet[StructField]() ++= schema.fields
    fieldSets.put(objId, fldSet)
  }

  def readAll() : DataFrame = {
    if (processSchema()) {
      val frames = new ArrayBuffer[DataFrame]()
      for (objid <- fieldSets.keySet) {
        val df = Spark.spark.read.parquet(objid)
        frames += normalizeDataframe(objid, df)
      }
      var result = frames(0)
      for (i <- 1 until frames.size)
        result = result.union(frames(i))
      result
    }
    else{
      //Если схемы не отличаются
      //То все фреймы зачитываются в 1 без каких либо
      //изменений
      Spark.spark.read.parquet(fieldSets.keySet.toList: _*)
    }
  }

  private def normalizeDataframe(objId:String, df:DataFrame) : DataFrame = {
    var newDf = df
    for (f <- toRename.get(objId).get){
      val columnName = f
      val newColumnName = columnName + "_" + objId.hashCode
      newDf = newDf.withColumnRenamed(columnName, newColumnName)
    }
    for (f <- toAppend.get(objId).get){
      newDf = newDf.withColumn(f.name, lit("").cast(f.dataType))
    }
    val sortedNames = newDf.columns.sortWith(_.compareTo(_) < 0)
    newDf.select(sortedNames.head, sortedNames.tail: _*)
  }

  //Возвращает true если хотя бы 2 схемы имеют отличия и необходим процесс сравнения
  //На выходе одновременно получаем сумму множеств полей,
  //отображение поле - набор объектов и результат поиска отличия в схемах
  private def prepareSum() : Boolean = {
    var isec = fieldSets.head._2
    val iter1 = fieldSets.iterator
    while (iter1.hasNext) {
      val p = iter1.next()
      val fields = p._2
      isec = isec.intersect(fields)
      val iter2 = fields.iterator
      while (iter2.hasNext) {
        val f = iter2.next()
        val objids = fieldsSum.get(f)
        if (objids.isDefined) objids.get += p._1
        else fieldsSum.put(f, new mutable.HashSet[String]() += p._1)
      }
    }
    !fieldsSum.keySet.diff(isec).isEmpty
  }

  private def processSchema() : Boolean = {
    toRename.clear()
    toAppend.clear()
    if (prepareSum()) {
      registerAllConflicts()
      val resultSchema = fieldsSum.keySet
      val iter = fieldSets.iterator
      while (iter.hasNext) {
        val p = iter.next()
        val diff = resultSchema.diff(p._2)
        mergeSchema(p._1, p._2, diff)
      }
      true
    }
    else false
  }

  private def registerAllConflicts() : Unit = {
    conflicts.clear()
    val sum = fieldsSum.keySet
    for (field <- sum) {
      val p = conflicts.get(field.name)
      if (p.isDefined) p.get += field
      else conflicts.put(field.name, new HashSet[StructField]() += field)
    }
  }

  private def mergeSchema(objid:String,
                          objFields:mutable.HashSet[StructField],
                          diff:collection.Set[StructField]) : Unit = {
    val rename = new ArrayBuffer[String]()
    val renamedAppend = new ArrayBuffer[StructField]()
    var append = new ArrayBuffer[StructField]()

    //Получаем список имен недостающих колонок
    val missingNames = new mutable.HashSet[String]()
    for (field <- diff)
      missingNames += field.name

    for (columnName <- missingNames){
      val ci = conflicts.get(columnName)
      if (ci.isDefined && ci.get.size > 1){
        //Если же конфликты есть, то необходимо добавить все переименованные колонки из гругих объектов
        //Если же текушая схема сама участвует в конфликте, то соответствующий столбец должен быть переименован
        for (cf <- ci.get){
          for (oi <- fieldsSum.get(cf).get){
            if (!oi.equals(objid)){
              val newName = columnName + "_" + oi.hashCode
              renamedAppend += StructField(newName, cf.dataType, true)
            }
            else rename += columnName
          }
        }
      }
      else{
        //Если это поле не вступает в конфликт ни по одной паре таблиц,
        //то просто добавляем его во фрейм
        append += ci.get.head
      }
    }
    toRename.put(objid, rename)
    toAppend.put(objid, append ++= renamedAppend)
  }

  private val fieldSets = new HashMap[String, mutable.HashSet[StructField]]()
  private val fieldsSum = new HashMap[StructField, mutable.HashSet[String]]
  private val toRename = new mutable.HashMap[String, mutable.ArrayBuffer[String]]()
  private val toAppend = new mutable.HashMap[String, ArrayBuffer[StructField]]()
  private val conflicts = new HashMap[String, HashSet[StructField]]()
}
