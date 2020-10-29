package com.asiainfo.DataStream

import org.apache.flink.api.common.state.StateTtlConfig.TimeCharacteristic
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.environment.CheckpointConfig.ExternalizedCheckpointCleanup
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.watermark.Watermark

/**
 * author: yhl
 * time: 2020/10/29 下午5:33
 * company: asiainfo
 */
object FlinkWaterMark {
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
    tupleStream.assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarks[(String, Long)] {

      override def getCurrentWatermark: Watermark = {

      }

      override def extractTimestamp(t: (String, Long), l: Long): Long = ???
    })

  }

}
