package com.abhi.utils.cassandra

import com.datastax.driver.core.BoundStatement
import com.abhi.utils.commandline
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema

import scala.collection.mutable


//Spark connector
import com.datastax.spark.connector.cql.CassandraConnector

object App {

  def main(args: Array[String]): Unit = {

    val argsUtil = new CLArgUtils(System.out).process(args)

    val host = argsUtil.getProperty("cassandra.host", "127.0.0.1")
    val uname = argsUtil.getProperty("cassandra.username", "cassandra")
    val pswd = argsUtil.getProperty("cassandra.password", "cassandra")
    val cluster = argsUtil.getProperty("cassandra.cluster", "west")
    val table = argsUtil.getProperty("cassandra.table", "documents")
    val keyspace = argsUtil.getProperty("cassandra.keyspace", "keyspace")
    val minColCount = argsUtil.getProperty("cassandra.minColCount", "50").toInt
    val partitionColumns: List[String] = argsUtil.getProperty("cassandra.table.partitionColumns", "doc_number,doc_type").split(",").toList.map(_.toUpperCase())
    val conditionColumn: String = argsUtil.getProperty("cassandra.table.conditionColumn", "sub_doc_type").toUpperCase()
    val columnValuesAllowed: Set[String] = argsUtil.getProperty("cassandra.table.columnValuesAllowed", "REGISTER,OCCUPIED,AVAILABLE,RETARDED").split(",").map(_.toUpperCase()).toSet
    val matchValueCountColumn: String = argsUtil.getProperty("cassandra.table.columnValuesAllowed.matchValueCountColumn", "AVAILABLE").toUpperCase()
    val deleteEnabled = argsUtil.getProperty("cassandra.table.deleteEnabled", "true").toBoolean
    val writePath = argsUtil.getProperty("row.dump.path", "hdfs:///user/deletePartitions")

    println(s"host $host")
    println(s"uname $uname")
    println(s"pswd $pswd")
    println(s"cluster $cluster")
    println(s"table $table")
    println(s"keyspace $keyspace")
    println(s"minColCount $minColCount")
    println(s"partitionColumns $partitionColumns")
    println(s"conditionColumn $conditionColumn")
    println(s"columnValuesAllowed $columnValuesAllowed")
    println(s"matchValueCountColumn $matchValueCountColumn")
    println(s"deleteEnabled $deleteEnabled")
    println(s"writePath $writePath")


    val groupByColumns: List[String] = partitionColumns :+ conditionColumn

    val conf = new SparkConf().setAppName("delete documents").setMaster("local")
      .set("spark.cassandra.connection.host", host)
      .set("spark.cassandra.auth.username", uname)
      .set("spark.cassandra.auth.password", pswd)

    val connector = CassandraConnector.apply(conf)


    val spark = SparkSession
      .builder()
      .config(conf)
      .appName("Spark")
      .getOrCreate()

    val gds = spark.read.format("org.apache.spark.sql.cassandra")
      .options(Map("table" -> table, "keyspace" -> keyspace, "cluster" -> cluster))
      .load()
      .groupBy(groupByColumns.head, groupByColumns.tail: _*)
      .count()
      .cache()

    gds.createOrReplaceTempView("documents_view")

    val docsGroupSql = s"select ${partitionColumns.mkString(", ")}" +
      s", collect_list(named_struct('$conditionColumn', $conditionColumn, 'count',count)) as DATA" +
      s" from documents_view group by ${partitionColumns.mkString(", ")}";

    val viewDF = spark.sql(docsGroupSql)

    viewDF.foreachPartition(rowIterator => {
      while (rowIterator.hasNext) {
        val row = rowIterator.next()
        val dataRow = row.get(row.fieldIndex("DATA")).asInstanceOf[mutable.WrappedArray.ofRef[AnyRef]]

        var colCounts = mutable.Map.empty[String, Long]
        for (i <- dataRow.indices) {
          val column_name: String = dataRow.array(i).asInstanceOf[GenericRowWithSchema].get(0).asInstanceOf[String]
          val count: Long = dataRow.array(i).asInstanceOf[GenericRowWithSchema].get(1).asInstanceOf[Long]
          colCounts += (column_name -> count)
        }

        val diffSet = colCounts.keySet.diff(columnValuesAllowed.map(_.toUpperCase()))
        if (diffSet.isEmpty &&
          colCounts.get(columnValueCount).nonEmpty && colCounts(columnValueCount) > minColCount) {
          println("TO DELETE")
          var partitionColMap = mutable.LinkedHashMap.empty[String, Any]


          val whereColStrings = partitionColumns.map(key => {
            val colVal = row.get(row.fieldIndex(key))
            partitionColMap += (key -> colVal)
            println(s"$key ${row.get(row.fieldIndex(key))}")
            (key +" =? ", colVal.asInstanceOf[Object])
          })

          if (deleteEnabled) {
            connector.withSessionDo(session => {
              //Statement is prepared and cache is hit instead of preparing on each row everytime
              val pstmt = session.prepare(s"DELETE FROM $keyspace.$table where ${whereColStrings.map(_._1).mkString(" and ")}")
              val bindValues = whereColStrings.map(_._2)

              val delBoundStmt = new BoundStatement(pstmt).bind( bindValues:_*)
              session.execute(delBoundStmt)
            })
          }

        }
      }
    })

  }


}
