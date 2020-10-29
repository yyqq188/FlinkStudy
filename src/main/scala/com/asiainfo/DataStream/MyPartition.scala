package com.asiainfo.DataStream

import org.apache.hadoop.mapred.{JobConf, Partitioner}

/**
 * author: yhl
 * time: 2020/10/29 下午4:41
 * company: asiainfo
 * 自定义分区   --- 这个待说
 */
class MyPartitioner extends Partitioner[String] {
  override def getPartition(k2: String, v2: V2, i: Int): Int = ???

  override def configure(jobConf: JobConf): Unit = ???
}
