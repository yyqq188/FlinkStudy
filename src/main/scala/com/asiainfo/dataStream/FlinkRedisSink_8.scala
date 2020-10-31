package com.asiainfo.dataStream

import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.connectors.redis.RedisSink
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig
import org.apache.flink.streaming.connectors.redis.common.mapper.{RedisCommand, RedisCommandDescription, RedisMapper}

/**
 * author: yhl
 * time: 2020/10/29 下午4:46
 * company: asiainfo
 */
object FlinkRedisSink {
  def main(args: Array[String]): Unit = {
    val environment:StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    import org.apache.flink.api.scala._
    val sourceStream:DataStream[String] = environment.fromElements("hello,world","spark,flink",
    "key,value")
    val tupleStream:DataStream[(String,String)] = sourceStream.map(
      x => {
        val strings:Array[String] = x.split(",")
        (strings(0),strings(1))
      }
    )
    val builder = new FlinkJedisPoolConfig.Builder
    val config : FlinkJedisPoolConfig = builder
      .setHost("node03")
      .setPort(6379)
      .setMaxIdle(10)
      .setMinIdle(2)
      .setTimeout(8000)
      .build()

      //获取redisSink
    val redisSink = new RedisSink[Tuple2[String,String]](config,new MyRedisMapper)
    tupleStream.addSink(redisSink)
    environment.execute()
  }
}

class MyRedisMapper extends RedisMapper[Tuple2[String,String]] {
  override def getCommandDescription: RedisCommandDescription = {
    new RedisCommandDescription(RedisCommand.SET)
  }

  override def getKeyFromData(data: (String, String)): String = {
    data._1
  }

  override def getValueFromData(data: (String, String)): String = {
    data._2
  }
}
