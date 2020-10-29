package com.asiainfo.DataStream

import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.windowing.time.Time

/**
 * author: yhl
 * time: 2020/9/22 下午4:07
 * company: asiainfo
 */
case class CountWord(word:String,count:Long)
object socketWordCount_1 {
  def main(args: Array[String]): Unit = {
    val environment:StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val result:DataStream[String] = environment.socketTextStream("localhost",9000)

    import org.apache.flink.api.scala._
    val count = result.flatMap(x => x.split(""))
      .map(x => CountWord(x,1))
      .keyBy("word")
      .timeWindow(Time.seconds(1),Time.milliseconds(1))
      .sum("count")
    count.print().setParallelism(1)
    environment.execute()
  }
}
