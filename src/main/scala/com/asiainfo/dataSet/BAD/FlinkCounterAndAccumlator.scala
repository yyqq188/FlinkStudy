package com.asiainfo.dataSet.BAD

import org.apache.flink.api.common.accumulators.LongCounter
import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.configuration.Configuration

/**
 * author: yhl
 * time: 2020/10/31 下午10:26
 * company: asiainfo
 */
object FlinkCounterAndAccumlator {
  def main(args: Array[String]): Unit = {
    val env:ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    import org.apache.flink.api.scala._
    val sourceDataSet:DataSet[String] = env.readTextFile("xx")
    sourceDataSet.map(new RichMapFunction[String,String] {
      var counter = new LongCounter()

      override def open(parameters: Configuration): Unit = {
        getRuntimeContext.addAccumulator("my-accountlator",counter)
      }
      override def map(value: String): String = {
        if(value.toLowerCase().contains("exception")) {
          counter.add(1)
        }
        value
      }
    }).setParallelism(4).writeAsText("xx")
    val job = env.execute()
    val a = job.getAccumulatorResult[Long]("my-accumulator")
    println(a)
  }

}
