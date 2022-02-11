package com.kuka.jd.kafka.consumer

import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.hadoop.hbase.client.{Admin, ColumnFamilyDescriptorBuilder, Connection, ConnectionFactory, Get, Put, Table, TableDescriptorBuilder}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.kafka.clients.consumer.{ConsumerRecords, KafkaConsumer}

import java.util
import java.util.{Collections, Properties}

abstract class jdConsumer {
  var hbaseConnection:Connection = _
  var infoTable:Table = _
  val tableName:String
  val families:Array[String]

  def setTable(host:String, port:String): Unit = {
    val hbaseConf = HBaseConfiguration.create()
    hbaseConf.set("hbase.zookeeper.quorum", host) //设置zooKeeper集群地址
    hbaseConf.set("hbase.zookeeper.property.clientPort", port) //设置zookeeper连接端口，默认2181
    this.hbaseConnection = ConnectionFactory.createConnection(hbaseConf)
    val admin = this.hbaseConnection.getAdmin
    if (!admin.tableExists(TableName.valueOf(this.tableName))) {
      // 创建表，注意hbase2.x的某个版本后弃用了HTableDescriptor和HColumnDescriptor，转而使用
      // TableDescriptorBuilder和ColumnFamilyDescriptorBuilder分别build出TableDescriptor和ColumnFamilyDescriptor的方式
      // 要进行设置，如MaxVersion等需要在builder上设置，创建出相应的实例以后就无法再设置了
      // 说实话完全看不出相比之前有什么好处，而且还更繁琐了
      val tableDescriptorBuilder = TableDescriptorBuilder.newBuilder(TableName.valueOf(this.tableName))
      this.families.foreach(family =>
        tableDescriptorBuilder.setColumnFamily(ColumnFamilyDescriptorBuilder.newBuilder(family.getBytes()).build()))
      val tableDescriptor = tableDescriptorBuilder.build()
      admin.createTable(tableDescriptor)
      admin.close()

    }
    this.infoTable = this.hbaseConnection.getTable(TableName.valueOf(this.tableName))

  }

  def startConsume(host:String, port:String): Unit

  def startConsume(host:String, port:String, family:String, colNames:Array[String]): Unit = {
    //kafka配置
    val prop = new Properties
    prop.put("bootstrap.servers", host + ":" + port)
    // 指定消费者组
    prop.put("group.id", family + "_group")
    // 指定消费位置: earliest/latest/none
    //    prop.put("auto.offset.reset", "earliest")
    // 指定消费的key的反序列化方式
    prop.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    // 指定消费的value的反序列化方式
    prop.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    prop.put("enable.auto.commit", "true")
    prop.put("session.timeout.ms", "30000")
    // 得到Consumer实例
    val kafkaConsumer = new KafkaConsumer[String, String](prop)
    // 首先需要订阅topic
    kafkaConsumer.subscribe(Collections.singletonList("jd_" + family))
    // 开始消费数据
    println(family + " consumer started")
    var temp = 0
    while (true) {
      // 如果Kafak中没有消息，会隔timeout这个值读一次。比如上面代码设置了2秒，也是就2秒后会查一次。
      // 如果Kafka中还有消息没有消费的话，会马上去读，而不需要等待。
      val msgs: ConsumerRecords[String, String] = kafkaConsumer.poll(2000)
      // println(msgs.count())


      val it = msgs.iterator()
      while (it.hasNext) {
        val msg = it.next()
        val rowkey = msg.key()
        val value = msg.value().split("\\|")
        println(s"partition: ${msg.partition()}, offset: ${msg.offset()}, key: $rowkey, value: ${msg.value()}")

        if (family.equals("list") && this.infoTable.get(new Get(Bytes.toBytes(rowkey))).value() != null) {
          temp += 1
          println("Find exist:" + rowkey + "," + temp + " exist now")
        }

        // 准备存入Hbase
//        try {
          val puts = new util.ArrayList[Put]()
          Array.range(0, colNames.length).foreach(i => {
            val put = new Put(Bytes.toBytes(rowkey))
            put.addColumn(Bytes.toBytes(family), Bytes.toBytes(colNames(i)), Bytes.toBytes(value(i)))
            puts.add(put)
          })
          this.infoTable.put(puts)
//        } catch {
//          case e:java.lang.ArrayIndexOutOfBoundsException => println()
//        }

      }
    }
  }
}
