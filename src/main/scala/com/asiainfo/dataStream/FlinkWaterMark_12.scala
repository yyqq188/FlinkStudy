package com.asiainfo.dataStream

import java.text.SimpleDateFormat

import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.scala.{DataStream, OutputTag, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import scala.collection.mutable.ArrayBuffer
import scala.util.Sorting

/**
 * author: yhl
 * time: 2020/10/30 上午10:25
 * company: asiainfo
 * 通过sideOutputLateData() 方法可以把延迟数据统一收集,存储
 */
object FlinkWaterMark_12 {
  def main(args: Array[String]): Unit = {
    val env:StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    import org.apache.flink.api.scala._
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)
    val text = env.socketTextStream("node01",9000)
    val inputMap:DataStream[(String,Long)] = text.map(line => {
      val arr = line.split(" ")
      (arr(0),arr(1).toLong)
    })


    val watermarkStream:DataStream[(String,Long)] = inputMap
      .assignTimestampsAndWatermarks(new AssignerWithPunctuatedWatermarks[(String, Long)] {
        var currentMaxTimestamp = 0L
        val watermarkDiff:Long = 10000L //基于eventTime向后推迟10秒
        val sdf:SimpleDateFormat = new SimpleDateFormat("YYYY-MM-DD HH:mm:ss.SSS")
        override def checkAndGetNextWatermark(t: (String, Long), l: Long): Watermark ={
          val watermark = new Watermark(currentMaxTimestamp - watermarkDiff)
          watermark
        }

        override def extractTimestamp(element: (String, Long), l: Long): Long = {
          val eventTime = element._2
          currentMaxTimestamp = Math.max(eventTime,currentMaxTimestamp)
          val id = Thread.currentThread().getId
          eventTime


        }
      })

    val outputTag:OutputTag[(String,Long)] = new OutputTag[(String,Long)]("late_data")
    val outputWindow:DataStream[String] = watermarkStream
      .keyBy(0)
      .window(TumblingEventTimeWindows.of(Time.seconds(3)))
      .sideOutputLateData(outputTag)
      .apply(new MyWindowFunction2)
    val sideOutput:DataStream[(String,Long)] = outputWindow.getSideOutput(outputTag)
    sideOutput.print()
    outputWindow.print()
    env.execute()
  }

}

class MyWindowFunction2 extends WindowFunction[(String,Long),String,Tuple,TimeWindow] {
  override def apply(key: Tuple,
                     window: TimeWindow,
                     input: Iterable[(String, Long)],
                     out: Collector[String]): Unit = {
    val keystr = key.toString
    val arrBuf = ArrayBuffer[Long]()
    val ite = input.iterator
    while (ite.hasNext) {
      val tup2 = ite.next()
      arrBuf.append(tup2._2)
    }
    val arr = arrBuf.toArray
    Sorting.quickSort(arr)
    val sdf = new SimpleDateFormat("YYYY-MM-DD HH:mm:ss.SSS")
    val result = ""
    out.collect(result)
  }
}