package com.asiainfo.DataStream.state.managed.keyed
import java.lang
import java.util.Collections

import org.apache.flink.api.common.functions.RichFlatMapFunction
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.util.Collector

/**
 * author: yhl
 * time: 2020/10/30 上午11:47
 * company: asiainfo
 * 这个状态为每一个key保存集合的值
 * get() 获得状态值
 * add()/addAll() 更新状态的值
 * clear() 清除状态
 */
object ListState {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    import org.apache.flink.api.scala._

    env.fromCollection(List(
      (1L,3d),
      (2L,3d),
      (1L,3d),
      (1L,3d),
      (1L,5d),
      (1L,3d),
      (1L,3d)
    ))
      .keyBy(_._1)
      .flatMap(new CountWindowAverageWithList)
      .print()
    env.execute()
  }
}

class CountWindowAverageWithList extends RichFlatMapFunction[(Long,Double),(Long,Double)] {
  private var elementsByKey:ListState[(Long,Double)] = _
  override def flatMap(element: (Long, Double), out: Collector[(Long, Double)]): Unit = {
    val currentState:lang.Iterable[(Long,Double)] = elementsByKey.get()
    if (currentState == null) {
      elementsByKey.addAll(Collections.emptyList())
    }
    elementsByKey.add(element)
    import scala.collection.JavaConverters._
    val allElements:Iterator[(Long,Double)] = elementsByKey.get().iterator().asScala
    val allElementList:List[(Long,Double)] = allElements.toList
    if (allElementList.size >= 3) {
      var count = 0L
      var sum = 0d
      for (eachElement <- allElementList) {
        count += 1
        sum += eachElement._2
      }
      out.collect(element._1,sum/count)
    }


  }

  override def open(parameters: Configuration): Unit = {
    //初始化获得历史状态值
    val listState = new ListStateDescriptor[(Long,Double)]("listState",classOf[(Long,Double)])
    elementsByKey = getRuntimeContext.getListState(listState)
  }
}