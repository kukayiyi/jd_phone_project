package com.kuka.jd.kafka.consumer

import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.hadoop.hbase.client.{Admin, ColumnFamilyDescriptorBuilder, Connection, ConnectionFactory, Get, Put, Table, TableDescriptorBuilder}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.kafka.clients.consumer.{ConsumerRecords, KafkaConsumer}

import java.util
import java.util.{Collections, Properties}

class commentConsumer extends jdConsumer {
  override val tableName: String = "jd_phone_comment"
  override val families: Array[String] = Array("comment")

  def this(host:String, port:String){
    this
    this.setTable(host, port)

  }

  override def startConsume(host:String, port:String): Unit ={
    val colNames = Array("id", "guid", "content", "creationTime",
      "score", "plusAvailable", "mobileVersion", "userClient")
    this.startConsume(host, port, "comment", colNames)
  }

}
