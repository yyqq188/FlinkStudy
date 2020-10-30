package com.asiainfo.DataStream.state.managed.keyed

import org.apache.flink.api.common.functions.{ReduceFunction, RichFlatMapFunction}
import org.apache.flink.api.common.state.{ReducingState, ReducingStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.util.Collector

/**
 * author: yhl
 * time: 2020/10/30 下午2:41
 * company: asiainfo
 * ReducingState用于每个key对应的历史数据进行聚合,例如可以通过ReducingState来实现对历史数据的累加
 */
object ReducingState {
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
      .flatMap(new CountWithReduceingAverageStage)
      .print()
    env.execute()
  }

}
class CountWithReduceingAverageStage extends RichFlatMapFunction[(Long,Double),(Long,Double)] {
  private var reducingState:ReducingState[Double] = _

  override def open(parameters: Configuration): Unit = {
    val reduceSum = new ReducingStateDescriptor[Double]("reduceSum",new ReduceFunction[Double] {
      override def reduce(t: Double, t1: Double): Double = {
        t + t1
      }
    },classOf[Double])
    reducingState = getIterationRuntimeContext.getReducingState(reduceSum)
  }
  override def flatMap(input: (Long, Double), output: Collector[(Long, Double)]): Unit = {
    reducingState.add(input._2)
    output.collect(input._1,reducingState.get())
  }
}