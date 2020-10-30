package com.asiainfo.DataStream.state.managed.keyed

import org.apache.flink.api.common.functions.RichFlatMapFunction
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.util.Collector

/**
 * author: yhl
 * time: 2020/10/30 上午11:33
 * company: asiainfo
 * 通过update()方法更新数据,通过value方法获得数据u
 */
object ValueState {
  def main(args: Array[String]): Unit = {
    val env:StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    import org.apache.flink.api.scala._
    env.fromCollection(List(
      (1L,3d),
      (1L,5d),
      (1L,7d),
      (1L,4d),
      (1L,2d)
    )).keyBy(_._1)
      .flatMap(new CountWindowAverage())
      .print()
    env.execute()
  }
}

class CountWindowAverage extends RichFlatMapFunction[(Long,Double),(Long,Double)] {
  private var sum:ValueState[(Long,Double)] = _

  override def flatMap(input: (Long, Double), output: Collector[(Long, Double)]): Unit = {
    val tmpCurrentSum = sum.value()
    val currentSum = if(tmpCurrentSum != null){
      tmpCurrentSum
    }else{
      (0L,0d)
    }
    val newSum = (currentSum._1 + 1,currentSum._2 + input._2)
    sum.update(newSum)
    if (newSum._1 >= 2){
      //如果计数达到2,则发出平均值并清除状态
      output.collect((input._1,newSum._2/newSum._1))
      //sum.clear()
    }

  }
  //获得历史状态的function
  override def open(parameters: Configuration): Unit = {
    sum = getRuntimeContext.getState(
      new ValueStateDescriptor[(Long, Double)]("average",classOf[(Long,Double)])
    )
  }
}
