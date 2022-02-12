import com.kuka.jd.kafka.consumer.commentConsumer
import org.apache.hadoop.hbase.client.{ConnectionFactory, Get, Put}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.kafka.clients.consumer.{ConsumerRecords, KafkaConsumer}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.junit.Test

import java.util.{Collections, Properties}

class testConsumer {
  @Test
  def testKafka():Unit ={
    val prop = new Properties
    prop.put("bootstrap.servers", "localhost:9092")
    // 指定消费者组
    prop.put("group.id", "group_comment")
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
    kafkaConsumer.subscribe(Collections.singletonList("jd_list"))
    // 开始消费数据
    println("Consumer started")
    while (true) {
      // 如果Kafak中没有消息，会隔timeout这个值读一次。比如上面代码设置了2秒，也是就2秒后会查一次。
      // 如果Kafka中还有消息没有消费的话，会马上去读，而不需要等2待。
      val msgs: ConsumerRecords[String, String] = kafkaConsumer.poll(2000)
      // println(msgs.count())
      val it = msgs.iterator()
      while (it.hasNext) {
        val msg = it.next()
        println(s"partition: ${msg.partition()}, offset: ${msg.offset()}, key: ${msg.key()}, value: ${msg.value()}")
      }
    }

  }

  @Test
  def testProducer(): Unit ={
    val properties = new Properties
    properties.put("bootstrap.servers","202.194.64.164:9092")
    properties.put("value.serializer","org.apache.kafka.common.serialization.StringSerializer")
    properties.put("key.serializer","org.apache.kafka.common.serialization.StringSerializer")
    //create new producer
    //    val producer: Producer[String, String] = new Producer[String,String](kafkaConfig)
    val producer = new KafkaProducer[String,String](properties)
    for(i <- 1 to 3)
    {
      println("send " + i)
      //      producer.send(new KeyedMessage[String,String]("test1",msg))
      producer.send(new ProducerRecord[String,String]("kafka_demo","key", i.toString))
    }

  }

  @Test
  def testInfo(): Unit ={
    val consumer = new commentConsumer("202.194.64.164", "2181")
    consumer.startConsume("202.194.64.164", "9092")
  }

  @Test
  def test(): Unit ={
    val hbaseConf = HBaseConfiguration.create()
    hbaseConf.set("hbase.zookeeper.quorum", "202.194.64.164") //设置zooKeeper集群地址
    hbaseConf.set("hbase.zookeeper.property.clientPort", "2181") //设置zookeeper连接端口，默认2181
    val hbaseConnection = ConnectionFactory.createConnection(hbaseConf)
    val admin = hbaseConnection.getAdmin
    val infoTable = hbaseConnection.getTable(TableName.valueOf("test"))
//    println(new String(infoTable.get(new Get(Bytes.toBytes("1000314060461"))).value()))
    val put = new Put(Bytes.toBytes("002"))
    put.addColumn(Bytes.toBytes("test"), Bytes.toBytes("col2"), Bytes.toBytes("00002"))
//    infoTable.put(put)
    val res = infoTable.get(new Get(Bytes.toBytes("001"))).getValue(Bytes.toBytes("test"), Bytes.toBytes("col1"))
    println()
  }

  @Test
  def testPro(): Unit ={
    val str = "a|b|c|||||d"
    val value = str.split("\\|")
    value.foreach(println)
  }


}
