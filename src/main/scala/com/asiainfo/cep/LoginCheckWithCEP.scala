package com.asiainfo.cep

import java.util

import org.apache.commons.lang3.time.FastDateFormat
import org.apache.flink.cep.PatternSelectFunction
import org.apache.flink.cep.pattern.conditions.IterativeCondition
import org.apache.flink.cep.scala.{CEP, PatternStream}
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala.{DataStream, KeyedStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.windowing.time.Time

import scala.collection.mutable

/**
 * author: yhl
 * time: 2020/10/31 上午6:47
 * company: asiainfo
 * 监测120秒内更换ip地址
 */

case class UserLogin(ip:String,username:String,url:String,time:String)



object LoginCheckWithCEP {
  private val format:FastDateFormat = FastDateFormat.getInstance("yyy-MM-dd HH:mm:ss.SSS")

  def main(args: Array[String]): Unit = {
    val env:StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    import org.apache.flink.api.scala._
    val sourceStream:DataStream[String] = env.socketTextStream("node01",9009)
    val result:KeyedStream[(String,UserLogin),String] = sourceStream.map(x => {
      val str:Array[String] = x.split(",")
      (str(1),UserLogin(str(0),str(1),str(2),str(3)))
    }).assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[(String, UserLogin)](Time.seconds(5)) {
      override def extractTimestamp(element: (String, UserLogin)): Long = {
        element._2.time
        val time:Long = format.parse(element._2.time).getTime
        time

      }
    }).keyBy(x => x._1)

    val pattern:Pattern[(String,UserLogin),(String,UserLogin)] = Pattern.begin[(String,UserLogin)]("begin")
      .where(x => {
        x._2.username != null
      }).next("second")
      .where(new IterativeCondition[(String,UserLogin)] {
        override def filter(value: (String, UserLogin),
                            context: IterativeCondition.Context[(String, UserLogin)]): Boolean = {
          var flag:Boolean = false
          val firstValues:java.util.Iterator[(String,UserLogin)] =
            context.getEventsForPattern("begin").iterator()
          while (firstValues.hasNext) {
            val tuple:(String,UserLogin) = firstValues.next()
            if(!tuple._2.ip.equals(value._2.ip)){
              flag = true
            }
          }
          flag
        }
      })
      .within(Time.seconds(120))
    val patternStream:PatternStream[(String,UserLogin)] = CEP.pattern(result,pattern)
    patternStream.select(new CEPPatternFunction).print()
    env.execute()
  }


}
class CEPPatternFunction extends PatternSelectFunction[(String,UserLogin),(String,UserLogin)] {
  override def select(map: java.util.Map[String, util.List[(String, UserLogin)]]): (String, UserLogin) = {
    val iter = map.get("begin").iterator()
    val tuple:(String,UserLogin) = map.get("second").iterator().next()
    import scala.collection.JavaConverters._
    val scalaIterable:Iterable[util.List[(String,UserLogin)]] = map.values().asScala
    for(eachIterable <- scalaIterable) {
      if (eachIterable.size()> 0) {
        val scalaListBuffer:mutable.Buffer[(String,UserLogin)] = eachIterable.asScala
        for(eachTuple <- scalaListBuffer) {
//          println(eachTuple._2)
        }
      }
    }
  tuple
  }
}