package com.asiainfo.DataStream

import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}

/**
 * author: yhl
 * time: 2020/10/29 下午4:34
 * company: asiainfo
 * partition算子
 */
object FlinkPartition_6 {
  def main(args: Array[String]): Unit = {
    val environment:StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    import org.apache.flink.api.scala._
    val dataStream:DataStream[String] = environment.fromElements("hello world","test spark",
    "abc hello","abcd hello","hell fink")
    val resultStream:DataStream[(String,Int)] = dataStream.filter(x => x.contains("hello"))
      .rebalance
      .flatMap(x => x.split(" "))
      .map(x => (x,1))
      .keyBy(0)
      .sum(1)
    resultStream.print().setParallelism(1)
    environment.execute()
  }
}
