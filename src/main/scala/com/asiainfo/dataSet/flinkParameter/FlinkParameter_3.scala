package com.asiainfo.dataSet.flinkParameter

import org.apache.flink.api.common.ExecutionConfig
import org.apache.flink.api.common.functions.RichFilterFunction
import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.configuration.Configuration

/**
 * author: yhl
 * time: 2020/10/31 下午5:24
 * company: asiainfo
 */
object FlinkParameter_3 {
  def main(args: Array[String]): Unit = {
    val configuration = new Configuration()
    configuration.setString("parameterKey","test")
    val env = ExecutionEnvironment.getExecutionEnvironment
    env.getConfig.setGlobalJobParameters(configuration)
    import org.apache.flink.api.scala._
    val sourceSet:DataSet[String] = env.fromElements("hello world","abc test")
    val filterSet:DataSet[String] = sourceSet.filter(new MyFilter1)
    filterSet.print()
    env.execute()
  }

}

class MyFilter1 extends RichFilterFunction[String] {
  var value:String = ""
  override def open(parameters: Configuration): Unit = {
    val parameters:ExecutionConfig.GlobalJobParameters = getRuntimeContext.getExecutionConfig
    val globalConf:Configuration = parameters.asInstanceOf[Configuration]
    value = globalConf.getString("parameterKey","test")
  }
  override def filter(t: String): Boolean = {
    if(t.contains(value)){
      true
    }else{
      false
    }
  }
}
