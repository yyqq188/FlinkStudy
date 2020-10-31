package com.asiainfo.dataStream

import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}

/**
 * author: yhl
 * time: 2020/10/29 下午4:15
 * company: asiainfo
 */
object UnionStream_3 {
  def main(args: Array[String]): Unit = {
    val environment:StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    import org.apache.flink.api.scala._
    val firstStream:DataStream[String] = environment.fromElements("hello,world","flink,spark")
    val secondStream:DataStream[String] = environment.fromElements("second,stream","hadoop,hive")
    val resultStream:DataStream[String] = firstStream.union(secondStream)
    resultStream.print().setParallelism(1)
    environment.execute()
  }
}
