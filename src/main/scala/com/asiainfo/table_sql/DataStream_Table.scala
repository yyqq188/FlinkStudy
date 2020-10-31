package com.asiainfo.table_sql

import org.apache.flink.core.fs.FileSystem.WriteMode
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.table.api.Table
import org.apache.flink.table.api.scala.StreamTableEnvironment
import org.apache.flink.table.sinks.CsvTableSink

/**
 * author: yhl
 * time: 2020/10/31 下午2:19
 * company: asiainfo
 */
case class User(id: String, name: String, age: String)
object DataStream_Table {
  def main(args: Array[String]): Unit = {
    val env:StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val streamSQLenv:StreamTableEnvironment = StreamTableEnvironment.create(env)
    val socketStream:DataStream[String] = env.socketTextStream("node01",9090)
    val userStream:DataStream[User] = socketStream.map(x=>User(x.split(",")(0),x.split(",")(1),x.split(",")(2).toInt))
    streamSQLenv.registerDataStream("userTable",userStream)

    val table:Table = streamSQLenv.sqlQuery("select * from userTable")
    val sink3 = new CsvTableSink("/home/aaa","===",1,WriteMode.OVERWRITE)
    //使用append模式将表装换为DataStream
    val appendStream:DataStream[User] = streamSQLenv.toAppendStream[User](table)
    //使用retract模式将表转化为DataStream
    val retractStream:DataStream[(Boolean,User)] = streamSQLenv.toRetractStream[User](table)
    env.execute()

  }
}
