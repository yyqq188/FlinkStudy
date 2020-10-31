package com.asiainfo.dataSet.flinkParameter

import org.apache.flink.api.common.functions.FilterFunction
import org.apache.flink.api.scala.ExecutionEnvironment

/**
 * author: yhl
 * time: 2020/10/31 下午5:13
 * company: asiainfo
 */
object FlinkParameter {
  def main(args: Array[String]): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    import org.apache.flink.api.scala._
    val sourceSet:DataSet[String] = env.fromElements("hello world","abc test")
    val filterSet:DataSet[String] = sourceSet.filter(new MyFilterFunction("test"))
    filterSet.print()
    env.execute()
  }

}
class MyFilterFunction(str: String) extends FilterFunction[String]{
  override def filter(t: String): Boolean = {
    if (t.contains(str)) {
      true
    }else{
      false
    }
  }
}