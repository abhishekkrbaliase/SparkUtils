package com.abhi.utils.cassandra

import com.datastax.driver.core.BoundStatement
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema

import scala.collection.mutable


//Spark connector
import com.datastax.spark.connector.cql.CassandraConnector

object App {
  val table = "documents"
  val keyspace = "keyspace"

  val minColCount = 0
  val partitionColumns: List[String] = List("doc_number", "doc_type").map(_.toUpperCase())
  val conditionColumn: String = "sub_doc_type".toUpperCase()

  val groupByColumns: List[String] =  partitionColumns :+ conditionColumn
  val columnValuesAllowed: Set[String] = Set("REGISTER", "OCCUPIED", "AVAILABLE", "RETARDED").map(_.toUpperCase())
  val columnValueCount: String = "AVAILABLE".toUpperCase()

  val deleteEnabled = true


  val host = "127.0.0.1"
  val uname = "cassandra"
  val pswd = "cassandra"
  //val cluster: String = "west"

  def main(args: Array[String]): Unit = {


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


    /* val x = spark.read.cassandraFormat(table, keyspace, cluster).load()
       .groupBy(columns.head, columns.tail: _*)
       .count()
       .where(s"count > $minCount")
       .sort(desc("count"))
       .cache()*/

  }


}
