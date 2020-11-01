package com.asiainfo.withKafka

import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend
import org.apache.flink.runtime.state.filesystem.FsStateBackend
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011

/**
 * author: yhl
 * time: 2020/11/1 上午9:32
 * company: asiainfo
 */
object FlinkKafkaSource {
  def main(args: Array[String]): Unit = {
    val env:StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    import org.apache.flink.streaming.api.CheckpointingMode
    import org.apache.flink.streaming.api.environment.CheckpointConfig
    env.enableCheckpointing(100)
    env.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE)
    env.getCheckpointConfig.setMinPauseBetweenCheckpoints(500)
    env.getCheckpointConfig.setCheckpointTimeout(60000)
    env.getCheckpointConfig.setMaxConcurrentCheckpoints(1)
    env.getCheckpointConfig.enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION)
    //将checkpoint保存到文件系统
//    env.setStateBackend(new FsStateBackend("hdfs://node01:8020/flink_state_save"))

    //将数据保存到RocksDB
    env.setStateBackend(new RocksDBStateBackend("hdfs://node01:9080/flink_state",true))
    import org.apache.flink.api.scala._
    val kafkaTopic:String = "test"
    val prop = new Properties()
    prop.setProperty("bootstrap.server","node01:9020")
    prop.setProperty("group.id","con1")
    prop.setProperty("key.deserializer","org.apache.kafka.common.serialization.StringDeserializer")
    prop.setProperty("value.deserializer","org.apache.kafka.common.serialization.StringDeserializer")
    val kafkaSource = new FlinkKafkaConsumer011[String](kafkaTopic,new SimpleStringSchema(),prop)
    //获取kafka中的数据
    val sourceStream:DataStream[String] = env.addSource(kafkaSource)
    sourceStream.print()
    env.execute()
  }
}
