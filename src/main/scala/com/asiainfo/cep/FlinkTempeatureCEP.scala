package com.asiainfo.cep

import java.util

import org.apache.commons.lang3.time.FastDateFormat
import org.apache.flink.cep.PatternSelectFunction
import org.apache.flink.cep.scala.{CEP, PatternStream}
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.{DataStream, KeyedStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.windowing.time.Time


/**
 * author: yhl
 * time: 2020/10/31 下午12:07
 * company: asiainfo
 * 3分钟之内出现3次以及3次以上温度高于40度的情况
 *
 */
case class DeviceDetail(sensorMac:String,deveceMac:String,tempeature:String,dampness:String,pressure:String,date:String)
case class AlarmDevice(sensorMac:String,deviceMac:String,tempeature:String)


object FlinkTempeatureCEP {
  private val format:FastDateFormat = FastDateFormat.getInstance("yyy-MM-dd HH:mm:ss")

  def main(args: Array[String]): Unit = {
    val env:StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)
    import org.apache.flink.api.scala._
    val sourceStream:DataStream[String] = env.socketTextStream("node01",9090)
    val deviceStream:KeyedStream[DeviceDetail,String] = sourceStream.map(x=>{
      val strings:Array[String] = x.split(",")
      DeviceDetail(strings(0),strings(1),strings(2),strings(3),string(4),strings(5))
    }).assignAscendingTimestamps(x => {
      format.parse(x.date).getTime

    }).keyBy(x => x.sensorMac)



    val pattern:Pattern[DeviceDetail,DeviceDetail] = Pattern.begin[DeviceDetail]("start")
      .where(x => x.tempeature.toInt >= 40)
      .followedByAny("follow")
      .where(x => x.tempeature.toInt >= 40)
      .followedByAny("third")
      .where(x => x.tempeature.toInt >= 40)
      .within(Time.minutes(3))


    val patternResult:PatternStream[DeviceDetail]= CEP.pattern(deviceStream,pattern)

    patternResult.select(new MyPatternResultFunction).print()
    env.execute("startTempeature")
  }



}
class MyPatternResultFunction extends PatternSelectFunction[DeviceDetail,AlarmDevice] {
  override def select(pattern: util.Map[String, util.List[DeviceDetail]]): AlarmDevice = {

    val startDetails:util.List[DeviceDetail] = pattern.get("start")
    val followDetails:util.List[DeviceDetail] = pattern.get("follow")
    val thirdDetails:util.List[DeviceDetail] = pattern.get("third")

    val startResult:DeviceDetail = startDetails.listIterator().next()
    val followResult:DeviceDetail = followDetails.listIterator().next()
    val thirdResult:DeviceDetail = thirdDetails.listIterator().next()

    AlarmDevice(thirdResult.sensorMac,thirdResult.deveceMac,thirdResult.tempeature)
  }
}
