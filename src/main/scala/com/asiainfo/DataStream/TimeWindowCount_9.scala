package com.asiainfo.DataStream

import org.apache.flink.api.common.functions.ReduceFunction
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.windowing.time.Time

/**
 * author: yhl
 * time: 2020/10/29 下午5:10
 * company: asiainfo
 */
object TimeWindowCount_9 {
  def main(args: Array[String]): Unit = {
    val environment:StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    import org.apache.flink.api.scala._
    val sourceStream: DataStream[String] = environment.socketTextStream(
      "node01",9000
    )
    val resultStream: DataStream[(Int,Int)] = sourceStream.map(x => (1,x.toInt))
      .keyBy(0)
      .timeWindow(Time.seconds(5))
      .reduce(new ReduceFunction[(Int, Int)] {
        override def reduce(t: (Int, Int), t1: (Int, Int)): (Int, Int) = {
          val result : Int = t1._2 + t._2
          (t1._1,result)
        }
      })

    resultStream.print()
    environment.execute()
  }
}
