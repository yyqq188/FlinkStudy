package com.asiainfo.table_sql

import org.apache.flink.api.scala.{DataSet, ExecutionEnvironment}
import org.apache.flink.core.fs.FileSystem.WriteMode
import org.apache.flink.table.api.scala.BatchTableEnvironment
import org.apache.flink.table.sinks.CsvTableSink

/**
 * author: yhl
 * time: 2020/10/31 下午2:30
 * company: asiainfo
 */
object DataSet_Table {
  def main(args: Array[String]): Unit = {
    val env:ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    val batchTableEnvironment:BatchTableEnvironment = BatchTableEnvironment.create(env)
    val sourceSet:DataSet[String] = env.readTextFile("xx")
    val userSet:DataSet[User3] = sourceSet.map(x =>
      User3(
        x.split(",")(0).toInt,
        x.split(",")(1),
        x.split(",")(2).toInt))
    import org.apache.flink.table.api._
    batchTableEnvironment.registerDataSet("user",userSet)
//    val table:Table = batchTableEnvironment.scan("user").filter("age>18")
    val table:Table = batchTableEnvironment.sqlQuery("select id,name,age from 'user'")
    val sink = new CsvTableSink("xx","===",1,WriteMode.OVERWRITE)
    table.writeToSink(sink)

    val tableSet:DataSet[User3] = batchTableEnvironment.toDataSet[User3](table)
    tableSet.map(x=>x.age).print()
    env.execute()

  }
}

case class User3(id:Int,name:String,age:Int)

