package com.asiainfo.DataStream

import org.apache.flink.streaming.api.scala.{ConnectedStreams, DataStream, StreamExecutionEnvironment}

/**
 * author: yhl
 * time: 2020/10/29 下午4:19
 * company: asiainfo
 * connect实现不同类型的流进行关联
 */
object ConnectionStream_4 {
  def main(args: Array[String]): Unit = {
    val environment:StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    import org.apache.flink.api.scala._
    val firstStream:DataStream[String] = environment.fromElements("hello,world","spark,flink")
    val secondStream:DataStream[Int] = environment.fromElements(1,2)
    val resultStream:ConnectedStreams[String,Int] = firstStream.connect(secondStream)
    val finalStream:DataStream[Any] = resultStream.map(x => {x + "abc"},y => {y * 10})
    finalStream.print().setParallelism(1)
    environment.execute()
  }
}
