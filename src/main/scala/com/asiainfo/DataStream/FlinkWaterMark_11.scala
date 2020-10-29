package com.asiainfo.DataStream

import java.text.SimpleDateFormat

import org.apache.flink.api.common.state.StateTtlConfig.TimeCharacteristic
import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.environment.CheckpointConfig.ExternalizedCheckpointCleanup
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks
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
 * time: 2020/10/29 下午5:33
 * company: asiainfo
 */
object FlinkWaterMark_11 {
  def main(args: Array[String]): Unit = {
    val environment:StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    //设置checkpoint的周期 每1000毫秒启动一个检查点
    environment.enableCheckpointing(1000)
    //设置模式为EXACTLY_ONCE 这也是默认设置
    environment.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE)
    //确保检查点之间是500毫秒
    environment.getCheckpointConfig.setMinPauseBetweenCheckpoints(500)
    //检车点必须在1分钟内完成,或者被丢弃(设置超时时间)
    environment.getCheckpointConfig.setCheckpointTimeout(60000)
    //同一时间只能有一个检车点 因为checkpoint也需要时间
    environment.getCheckpointConfig.setMaxConcurrentCheckpoints(1)
    //程序取消,保留检查点信息 ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION
    //程序取消,保留检查点信息 ExternalizedCheckpointCleanup.DELETE_ON_CANCELLATION

    environment.getCheckpointConfig.enableExternalizedCheckpoints(ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION)
    //设置checkpoint保存的地址
//    environment.setStateBackend(new MemoryStateBackend()) //将数据保存到内存中
//    environment.setStateBackend(new FsStateBackend("hdfs://node01:8000/flink_state_save"))
//    environment.setStateBackend(new RocksDBStateBackend("hdfs://node01:8020/flink_save_checkpint/checkDir",true))

    import org.apache.flink.api.scala._
    environment.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    environment.setParallelism(1)

    val sourceStream:DataStream[String] = environment.socketTextStream("node01",9000).uid("my_source")
    val tupleStream:DataStream[(String,Long)] = sourceStream.map(x => {
      val strings:Array[String] = x.split(",")
      (strings(0),strings(1).toLong)
    }).uid("mapuid")
    //给数据注册watermark
    val waterMarkStream:DataStream[(String,Long)] = tupleStream.assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarks[(String, Long)] {
      var currentTimemillis:Long = 0L
      var timeDiff:Long = 10000L
      val sdf = new SimpleDateFormat("yyqq-MM-dd HH:mm:ss.SSS")

      override def getCurrentWatermark: Watermark = {
        val watermark = new Watermark((currentTimemillis - timeDiff))
        watermark

      }

      override def extractTimestamp(element: (String, Long), l: Long): Long = {
        currentTimemillis = Math.max(currentTimemillis,element._2)
        val id = Thread.currentThread().getId
        element._2 //获得EventTIme然后返回
      }
    })
      .keyBy(0)
      .window(TumblingEventTimeWindows.of(Time.seconds(10)))
      .allowedLateness(Time.seconds(2))
      .apply(new MyWindowFunction)
      .print()

    val outputTag:OutputTag[(String,Long)] = new OutputTag[(String, Long)]("late_date")
    val outputWindow:DataStream[String] = waterMarkStream
      .keyBy(0)
      .window(TumblingEventTimeWindows.of(Time.seconds(3)))
      .sideOutputLateData(outputTag)
      .apply(new MyWindowFunction)
    val sideOutput:DataStream[(String,Long)] = outputWindow.getSideOutput(outputTag)
    sideOutput.print()
    outputWindow.print()
    environment.execute()
  }

}

class MyWindowFunction extends WindowFunction[(String,Long),String,Tuple,TimeWindow]{
  //窗口里面所有的数据都封装在input中
  //输出的数据都是通过out进行输出的
  override def apply(key: Tuple,
                     window: TimeWindow,
                     input: Iterable[(String, Long)],
                     out: Collector[String]): Unit = {
    window.getStart
    window.getEnd
    val keyStr = key.toString
    val arrBuf = ArrayBuffer[Long]()
    val ite = input.iterator
    while (ite.hasNext) {
      val tup2 = ite.next()
      arrBuf.append(tup2._2)
    }

    val arr = arrBuf.toArray
    Sorting.quickSort(arr) //按照EventTime对数据进行排序
    val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS")
    val result = ""
    out.collect(result)

  }
}