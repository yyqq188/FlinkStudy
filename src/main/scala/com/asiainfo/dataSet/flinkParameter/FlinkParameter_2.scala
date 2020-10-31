package com.asiainfo.dataSet.flinkParameter

import org.apache.flink.api.common.functions.RichFilterFunction
import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.configuration.Configuration

/**
 * author: yhl
 * time: 2020/10/31 下午5:18
 * company: asiainfo
 */
object FilterParameter_2 {
  def main(args: Array[String]): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    import org.apache.flink.api.scala._
    val sourceSet:DataSet[String] = env.fromElements("hello world","test test")
    val configuration = new Configuration()
    configuration.setString("parameterKey","test")
    val filterSet:DataSet[String] = sourceSet.filter(new MyFilter).withParameters(configuration)
    filterSet.print()
    env.execute()
  }

}

class MyFilter extends RichFilterFunction[String] {
  var value:String = ""

  override def open(parameters: Configuration): Unit = {
    value = parameters.getString("parameterKey","defaultValue")

  }

  override def filter(t: String): Boolean = {
    if (t.contains(value)) {
      true
    }else{
      false
    }
  }
}
