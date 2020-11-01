package com.asiainfo.withKafka

import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer011
import org.apache.flink.streaming.connectors.kafka.internals.KeyedSerializationSchemaWrapper

/**
 * author: yhl
 * time: 2020/11/1 上午9:44
 * company: asiainfo
 */
object FlinkKafkaSink {
  def main(args: Array[String]): Unit = {
    val env:StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    import org.apache.flink.contrib.streaming.state.RocksDBStateBackend
    import org.apache.flink.streaming.api.CheckpointingMode
    import org.apache.flink.streaming.api.environment.CheckpointConfig
    //checkpoint配置
    env.enableCheckpointing(5000)
    env.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE)
    env.getCheckpointConfig.setMinPauseBetweenCheckpoints(500)
    env.getCheckpointConfig.setMaxConcurrentCheckpoints(1)
    env.getCheckpointConfig.setCheckpointTimeout(60000)
    env.getCheckpointConfig.enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION)

    //设置StateBackend
    env.setStateBackend(new RocksDBStateBackend("hdfs://node01:8800/flink_kafka_sink/checkpoint",true))

    val sourceStream:DataStream[String] = env.socketTextStream("node01",9000)
    val prop = new Properties()
    prop.setProperty("bootstrap.servers","node01:9090")
    prop.setProperty("group.id","kafka_group1")
    //设置FlinkKafkaProducer011事务超时时间
    prop.setProperty("transaction.timeout.ms",60000 * 15 + "")
    new FlinkKafkaProducer011[String]("test",new KeyedSerializationSchemaWrapper[String](new SimpleStringSchema()),prop)

    //将数据写入kafka
    sourceStream.print()
    env.execute()
  }
}
