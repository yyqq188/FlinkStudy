package com.asiainfo.cep

import java.util

import org.apache.commons.lang3.time.FastDateFormat
import org.apache.flink.cep.{PatternSelectFunction, PatternTimeoutFunction}
import org.apache.flink.cep.scala.{CEP, PatternStream}
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala.{DataStream, KeyedStream, OutputTag, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.windowing.time.Time

/**
 * author: yhl
 * time: 2020/10/31 下午12:31
 * company: asiainfo
 * 支付超时CEP,15分钟内要付款
 */

case class OrderDetail(orderId:String,status:String,orderCreateTime:String,price:Double)
object OrderTimeOutCheckCEP {
  private val format:FastDateFormat = FastDateFormat.getInstance("yyy-MM-dd HH:mm:ss.SSS")

  def main(args: Array[String]): Unit = {
    val env:StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)
    import org.apache.flink.api.scala._
    val sourceStream:DataStream[String] = env.socketTextStream("node01",9090)
    val deviceStream:KeyedStream[OrderDetail,String] = sourceStream.map(x => {
      val strings:Array[String] = x.split(",")
      OrderDetail(strings(0),strings(1),strings(2),strings(3).toDouble)
    }).assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[OrderDetail](Time.seconds(5)) {
      override def extractTimestamp(t: OrderDetail): Long = {
        format.parse(t.orderCreateTime).getTime
      }
    }).keyBy(x => x.orderId)

    val pattern:Pattern[OrderDetail,OrderDetail] = Pattern.begin[OrderDetail]("start")
      .where(order => order.status.equals("1"))
      .followedBy("second")
      .where(x=>x.status.equals("2"))
      .within(Time.minutes(15))

    val patternStream:PatternStream[OrderDetail] = CEP.pattern(deviceStream,pattern)

    //设置告警
    val orderTimeoutputTag = new OutputTag[OrderDetail]("orderTimeoutS")

    val selectResultStream:DataStream[OrderDetail] = patternStream.select(orderTimeoutputTag,new OrderTimeOutPatternFunction,new OrderTimeOutPatternFunction)
    selectResultStream.getSideOutput(orderTimeoutputTag).print()
    env.execute()

  }
}

class OrderTimeOutPatternFunction extends PatternTimeoutFunction[OrderDetail,OrderDetail] {
  override def timeout(pattern: util.Map[String, util.List[OrderDetail]], l: Long): OrderDetail = {
    val detail: OrderDetail = pattern.get("start").iterator().next()
    println("超时订单号"+detail)
    detail
  }
}

class OrderPatternFunction extends PatternSelectFunction[OrderDetail,OrderDetail] {
  override def select(map: util.Map[String, util.List[OrderDetail]]): OrderDetail = {
    val detail:OrderDetail = map.get("second").iterator().next()
    println("支付成功的订单为" + detail)
    detail
  }
}