package com.asiainfo.withHbase

import org.apache.flink.addons.hbase.TableInputFormat
import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.hadoop.hbase.{Cell, HBaseConfiguration, HConstants, TableName}
import org.apache.hadoop.hbase.client.{Connection, ConnectionFactory, HTable, Result, Scan}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.flink.api.java.tuple
/**
 * author: yhl
 * time: 2020/11/1 上午10:10
 * company: asiainfo
 */
object FlinkReadHbase {
  def main(args: Array[String]): Unit = {
    val env:ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    import org.apache.flink.api.scala._

    val HbaseData:DataSet[tuple.Tuple2[String,String]] = env.createInput(new TableInputFormat[tuple.Tuple2[String,String]] {
      val conf = HBaseConfiguration.create()
      conf.set(HConstants.ZOOKEEPER_QUORUM,"node01,node02,node03")
      conf.set(HConstants.ZOOKEEPER_CLIENT_PORT,"2181")
      val conn : Connection = ConnectionFactory.createConnection(conf)
      val table = classOf[HTable].cast(conn.getTable(TableName.valueOf("hbasesource")))

      val scan = new Scan() {
        addFamily(Bytes.toBytes("f1"))
      }
      override def getScanner: Scan = {
        scan
      }

      override def getTableName: String = {
        "hbasesource"
      }

      override def mapResultToTuple(result: Result): tuple.Tuple2[String, String] = {
        val rowkey:String = Bytes.toString(result.getRow)
        val sb = new StringBuffer()
        for(cell:Cell <- result.rawCells()){
          val value = Bytes.toString(cell.getValueArray,cell.getValueOffset,cell.getValueLength)
          sb.append(value).append(",")
        }
        val valueString = sb.replace(sb.length() - 1,sb.length(),"").toString
        val tuple2 = new org.apache.flink.api.java.tuple.Tuple2[String,String]
        tuple2.setField(rowkey,0)
        tuple2.setField(valueString,1)
        tuple2
      }
    })

    HbaseData.print()
    env.execute()
  }
}
