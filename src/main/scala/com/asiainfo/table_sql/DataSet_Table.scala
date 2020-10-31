package com.asiainfo.table_sql

import org.apache.flink.api.scala.{DataSet, ExecutionEnvironment}
import org.apache.flink.table.api.scala.BatchTableEnvironment

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
    val userSet:DataSet[User3] = sourceSet.map(x => User3(x.split(",")(0).toInt,x.split(",")(1),x.split(",")(2).toInt))
    import org.apache.flink.table.api._
    batchTableEnvironment.registerDataSet("user",userSet)


  }
}

case class User3(id:Int,name:String,age:Int)

