package com.asiainfo.table_sql

import org.apache.flink.core.fs.FileSystem.WriteMode
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.{Table, Types}
import org.apache.flink.table.api.scala.StreamTableEnvironment
import org.apache.flink.table.sinks.CsvTableSink
import org.apache.flink.table.sources
import org.apache.flink.table.sources.CsvTableSource

/**
 * author: yhl
 * time: 2020/10/31 下午2:03
 * company: asiainfo
 */
object FlinkStreamSQL {
  val env:StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
  val tablenv:StreamTableEnvironment=   StreamTableEnvironment.create(env)
  val source:CsvTableSource = CsvTableSource.builder()
    .field("id",Types.INT)
    .field("name",Types.STRING)
    .field("age",Types.INT)
    .fieldDelimiter(",")
    .ignoreFirstLine()
    .ignoreParseErrors()
    .lineDelimiter("\r\n")
    .path("sss")
    .build()

  tablenv.registerTableSource("user",source)
  val result:Table = tablenv.scan("user").filter("age>18")
  val sink = new CsvTableSink("xx","===",1,WriteMode.OVERWRITE)
  result.writeToSink(sink)
  env.execute()
}
