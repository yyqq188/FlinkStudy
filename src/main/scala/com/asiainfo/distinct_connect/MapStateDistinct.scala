package com.asiainfo.distinct_connect

import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011
import org.apache.kafka.clients.consumer.ConsumerConfig

/**
 * author: yhl
 * time: 2020/11/1 下午8:23
 * company: asiainfo
 * 基于MapState实现流式去重
 */
//定义case class 用于包装kafka中的数据
case class AdData(id:Int,devId:String,time:Long)
case class AdKey(id:Int,time:Long)
object MapStateDistinct {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    val kafkaConf = new Properties()
    kafkaConf.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,"node01:9092")
    kafkaConf.put(ConsumerConfig.GROUP_ID_CONFIG,"flink_kafka_group")
    val consumer = new FlinkKafkaConsumer011[String]("flink_kafka",new SimpleStringSchema,kafkaConf)

    val ds = env.addSource(consumer)
      .map(x => {
        val s = x.split(",")
        AdData(s(0).toInt,s(1),s(2).toLong)
      }).assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[AdData](Time.minutes(1)) {
      override def extractTimestamp(t: AdData): Long = {
        t.time
      }
    }).keyBy(x => {
      val endTime = TimeWindow.getWindowStartWithOffset(x.time,0,Time.hours(1).toMilliseconds) + Time.hours(1).toMilliseconds
      AdKey(x.id,endTime)
    })



  }


}
