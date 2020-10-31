package com.asiainfo.dataSet.BAD

import org.apache.commons.io.FileUtils
import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.configuration.Configuration

/**
 * author: yhl
 * time: 2020/10/31 下午10:31
 * company: asiainfo
 */
object FlinkDistributedCache {
  def main(args: Array[String]): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    import org.apache.flink.api.scala._

    env.registerCachedFile("xxx","advert")
    val data = env.fromElements("hello","flink")
    val result = data.map(new RichMapFunction[String,String] {
      override def open(parameters: Configuration): Unit = {
        super.open(parameters)
        val myFile = getRuntimeContext.getDistributedCache.getFile("advert")
        val lines = FileUtils.readLines(myFile)
        val ite = lines.iterator()
        while (ite.hasNext){
          val line = ite.next()
          println("line" + line)
        }
      }
      override def map(in: String): String = {
          in
      }
    }).setParallelism(2)
    result.print()
    env.execute()
  }

}
