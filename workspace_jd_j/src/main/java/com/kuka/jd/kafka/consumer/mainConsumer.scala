package com.kuka.jd.kafka.consumer

import com.kuka.jd.DataProcessing.util.PropertiesReader


import scala.io.StdIn
import scala.util.control.Breaks.{break, breakable}

object mainConsumer {
  var consumer:jdConsumer = _
  def main(args: Array[String]): Unit = {
    val pro:PropertiesReader = new PropertiesReader()
    val host = pro.getValue("host.master")
    val hbasePort = pro.getValue("port.zookeeper")
    val kafkaPort = pro.getValue("port.kafka")
    while(true){
      println("请输入要执行的消费者，1：手机列表消费 2：手机详情消费 3：评论消费")
      breakable {
        StdIn.readLine() match {
          case "1" => consumer = new infoConsumer(host, hbasePort)
          case "2" => consumer = new detailConsumer(host, hbasePort)
          case "3" => consumer = new commentConsumer(host, hbasePort)
          case _ => println("输入错误，请重新输入")
                    break()
        }
        consumer.startConsume(host, kafkaPort)
      }


    }

  }

}
