package com.kuka.jd.kafka.consumer

import org.apache.hadoop.hbase.client.{Get, Put}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.kafka.clients.consumer.{ConsumerRecords, KafkaConsumer}

import java.util
import java.util.{Collections, Properties}

class detailConsumer extends jdConsumer {
  override val tableName: String = "jd_phone_info"
  override val families: Array[String] = Array("list", "detail")

  def this(host:String, port:String){
    this
    this.setTable(host, port)

  }

  override def startConsume(host: String, port: String): Unit = {
     val colNames = Array("name", "gross_weight",
       "origin_place", "cpu", "ram" ,"storage", "storage_card",
       "camera_num", "rear_px", "front_px", "screen_size",
       "resolution", "screen_ratio", "charger", "hotspot",
       "os", "game_config", "brand")
     this.startConsume(host, port, "detail", colNames)

  }
}
