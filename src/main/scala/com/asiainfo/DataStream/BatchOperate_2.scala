package com.asiainfo.DataStream

import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.core.fs.FileSystem.WriteMode

/**
 * author: yhl
 * time: 2020/9/22 下午4:28
 * company: asiainfo
 */
object BatchOperate_2 {
  def main(args: Array[String]): Unit = {
    val environment:ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    import org.apache.flink.api.scala._
    val fileDataSet:DataSet[String] = environment.readTextFile("xxx")
    val resultDataSet:AggregateDataSet[(String,Int)] =
      fileDataSet.flatMap(x => x.split(" "))
        .map(x => (x,1)).groupBy(0).sum(1)

    resultDataSet.writeAsText("xxxx",WriteMode.OVERWRITE)
    environment.execute()
  }
}
