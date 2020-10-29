package com.asiainfo.DataStream

import java.{lang, util}

import org.apache.flink.streaming.api.collector.selector.OutputSelector
import org.apache.flink.streaming.api.scala.{DataStream, SplitStream, StreamExecutionEnvironment}

/**
 * author: yhl
 * time: 2020/10/29 下午4:24
 * company: asiainfo
 * 切分流
 */
object SplitStream_5 {
  def main(args: Array[String]): Unit = {
    val environment:StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    import org.apache.flink.api.scala._
    val sourceStream:DataStream[String] = environment.fromElements("hello,world","spark,flink",
    "hadoop,hive")
    val splitStream:SplitStream[String] = sourceStream.split(new OutputSelector[String] {
      override def select(out: String): lang.Iterable[String] = {
        val strings = new util.ArrayList[String]()
        if (out.contains("hello")) {
          strings.add("stream1")
        }else{
          strings.add("stream2")
        }
        strings

      }
    })
    val helloStream:DataStream[String] = splitStream.select("stream1")
    helloStream.print().setParallelism(1)
    environment.execute()
  }
}
