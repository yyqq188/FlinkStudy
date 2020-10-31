package com.asiainfo.dataStream

import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.streaming.api.datastream.DataStreamSink
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow
import org.apache.flink.util.Collector

/**
 * author: yhl
 * time: 2020/10/29 下午5:18
 * company: asiainfo
 * 全量统计
 *
 */
object CountWindowAvg {
  def main(args: Array[String]): Unit = {
    val environment:StreamExecutionEnvironment= StreamExecutionEnvironment.getExecutionEnvironment
    import org.apache.flink.api.scala._

    val sourceStream:DataStream[String] = environment.socketTextStream("node01",8990)
    val avgResult:DataStreamSink[Double] =    sourceStream.map(x => (1,x.toInt)).keyBy(0)
        .countWindow(3)
        .process(new MyProcessWindow)
        .print()


    environment.execute()
  }
}


class MyProcessWindow extends ProcessWindowFunction[(Int,Int),Double,Tuple,GlobalWindow] {
  override def process(key: Tuple, context: Context,
                       elements: Iterable[(Int, Int)],
                       out: Collector[Double]): Unit = {
    var totalNum:Int = 0;
    var totalResult:Int = 0;
    for (element <- elements) {
      totalNum += 1
      totalResult += element._2

    }
    out.collect(totalResult/totalNum)
  }
}
