package com.asiainfo.dataSet.BAD

import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}

/**
 * author: yhl
 * time: 2020/10/31 下午5:54
 * company: asiainfo
 */
object FlinkBroadCast {
  def main(args: Array[String]): Unit = {
    val env:StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(4)
    import org.apache.flink.api.scala._
    val result:DataStream[String] = env.fromElements("hello")
      .setParallelism(1)
    val resultValue:DataStream[String] = result.broadcast.map(x => {
      println(x)
      x
    })
    resultValue.print()
    env.execute()
  }
}
