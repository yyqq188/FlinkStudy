package com.asiainfo.dataSet.BAD

import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.configuration.Configuration

import scala.collection.mutable

/**
 * author: yhl
 * time: 2020/10/31 下午10:13
 * company: asiainfo
 */
object FlinkDataSetBroadcast {
  def main(args: Array[String]): Unit = {
    val env:ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    import org.apache.flink.api.scala._
    val productData:DataSet[String] = env.readTextFile("xx")
    val productMap = new mutable.HashMap[String,String]()
    val productMapSet:DataSet[mutable.HashMap[String,String]] = productData.map(x => {
      val strings:Array[String] = x.split(",")
      productMap.put(strings(0),x)
      productMap
    })
    val ordersDataSet:DataSet[String] = env.readTextFile("xx")
    val resultLine:DataSet[String] = ordersDataSet.map(new RichMapFunction[String,String] {
      var listData:java.util.List[Map[String,String]] = null
      var allMap = Map[String,String]()

      override def open(parameters: Configuration): Unit = {
        this.listData = getRuntimeContext.getBroadcastVariable[Map[String,String]]("productBroadCast")
        val listResult:java.util.Iterator[Map[String,String]] = listData.iterator()
        while(listResult.hasNext){
          allMap= allMap.++(listResult.next())
        }
      }
      override def map(eachOrder: String): String = {
        val str:String = allMap.getOrElse(eachOrder.split(",")(2),"暂时没有值")
        eachOrder + "," + str


      }
    }).withBroadcastSet(productMapSet,"productBroadCast")
    resultLine.print()
    env.execute("broadcast")
  }

}
