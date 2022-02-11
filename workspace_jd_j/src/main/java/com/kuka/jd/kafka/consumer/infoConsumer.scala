package com.kuka.jd.kafka.consumer

import org.apache.hadoop.hbase.client.{Get, Put, Table}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.kafka.clients.consumer.{ConsumerRecords, KafkaConsumer}

import java.util
import java.util.{Collections, Properties}
import scala.collection.mutable.ArrayBuffer

class infoConsumer extends jdConsumer {
  override val tableName: String = "jd_phone_info"
  override val families: Array[String] = Array("list", "detail")

  def this(host:String, port:String){
    this
    this.setTable(host, port)

  }

  override def startConsume(host: String, port: String): Unit = {
     val colNames = Array("phone_name", "phone_url", "phone_price",
       "phone_comment_cnt", "phone_seller" ,"phone_proprietary")
     this.startConsume(host, port, "list", colNames)

  }

}
