package com.asiainfo.DataStream.state.managed.keyed

import java.util.UUID

import org.apache.flink.api.common.functions.RichFlatMapFunction
import org.apache.flink.api.common.state.{MapState, MapStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.util.Collector

/**
 * author: yhl
 * time: 2020/10/30 下午2:24
 * company: asiainfo
 * map是一个map,用户通过put() 或 putAll()添加元素
 */
object MapState {
  def main(args: Array[String]): Unit = {
    val env:StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    import org.apache.flink.api.scala._
    env.fromCollection(List(
      (1L,3d),
      (1L,3d),
      (1L,3d),
      (1L,3d),
      (1L,3d),
      (1L,3d),
      (1L,3d),
      (1L,3d)
    )).keyBy(_._1)
      .flatMap(new CountWithAverageMapState)
      .print()
    env.execute()
  }

}

class CountWithAverageMapState extends RichFlatMapFunction[(Long,Double),(Long,Double)] {
  private var mapState:MapState[String,Double] = _
  override def open(parameters: Configuration): Unit = {
    val mapStateOperate = new MapStateDescriptor[String,Double]("mapStateOperate",classOf[String],classOf[Double])
    getRuntimeContext.getMapState(mapStateOperate)
  }
  override def flatMap(input: (Long, Double), output: Collector[(Long, Double)]): Unit = {
    //将相同的key对应的数据放到一个map集合中
    mapState.put(UUID.randomUUID().toString,input._2)
    import scala.collection.JavaConverters._
    val listState = mapState.values().iterator().asScala.toList
    if (listState.size >= 3) {
      var count = 0L
      var sum = 0d
      for (eachState <- listState) {
        count += 1
        sum += eachState
      }
      println("average"+sum/count)
      output.collect(input._1,sum/count)
    }
  }
}
