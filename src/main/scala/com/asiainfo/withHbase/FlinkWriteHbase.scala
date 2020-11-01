package com.asiainfo.withHbase

import java.util

import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.hadoop.hbase.client.{BufferedMutator, BufferedMutatorParams, Connection, ConnectionFactory, Put}
import org.apache.flink.api.common.io.OutputFormat
import org.apache.flink.configuration.Configuration
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HBaseConfiguration, HConstants, TableName}
/**
 * author: yhl
 * time: 2020/11/1 上午10:27
 * company: asiainfo
 */
object FlinkWriteHbase {
  def main(args: Array[String]): Unit = {
    val env:ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    import org.apache.flink.api.scala._
    val sourceDataSet:DataSet[String] = env.fromElements("01,zhangsan,28","02,lisi,30")
    sourceDataSet.output(new HbaseOutputFormat)
    env.execute()
  }

}

class HbaseOutputFormat extends OutputFormat[String] {
  val zkServer = "node01"
  val port = "2181"
  var conn:Connection = null
  override def configure(configuration: Configuration): Unit = {

  }

  override def open(i: Int, i1: Int): Unit = {
    val conf:org.apache.hadoop.conf.Configuration = HBaseConfiguration.create()
    conf.set(HConstants.ZOOKEEPER_QUORUM,zkServer)
    conf.set(HConstants.ZOOKEEPER_CLIENT_PORT,port)
    conf.setInt(HConstants.HBASE_CLIENT_OPERATION_TIMEOUT,30000)
    conf.setInt(HConstants.HBASE_CLIENT_SCANNER_TIMEOUT_PERIOD,30000)
    conn = ConnectionFactory.createConnection(conf)

  }

  override def writeRecord(it: String): Unit = {
    val tableName:TableName = TableName.valueOf("hbasesource")
    val cf1 = "f1"
    val array:Array[String] = it.split(",")
    val put:Put = new Put(Bytes.toBytes(array(0)))
    put.addColumn(Bytes.toBytes(cf1),Bytes.toBytes("age"),Bytes.toBytes(array(1)))
    put.addColumn(Bytes.toBytes(cf1),Bytes.toBytes("age"),Bytes.toBytes(array(2)))
    val putList:java.util.ArrayList[Put] = new util.ArrayList[Put]
    putList.add(put)

    //设置缓存1mb,当达到1mb时数据就会自动刷到Hbase
    val params:BufferedMutatorParams = new BufferedMutatorParams(tableName)
    params.writeBufferSize(1024 * 1024)
    val mutator:BufferedMutator  = conn.getBufferedMutator(params)
    mutator.mutate(putList)
    mutator.flush()
    mutator.close()
  }

  override def close(): Unit = {
    if (null != conn) {
      conn.close()
    }
  }
}