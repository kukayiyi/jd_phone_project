package com.kuka.jd.DataProcessing

import com.csvreader.CsvWriter
import com.kuka.jd.DataProcessing.dao.hiveTableDao
import com.kuka.jd.DataProcessing.util.{PropertiesReader, SessionBuilder}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.hbase.client.{ConnectionFactory, Scan}
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}

import java.net.URI
import java.nio.charset.Charset

class TableConversion {
//  def HbaseToHive(host:String, tableName:String, hbaseColName: Array[String], hiveColName:Array[String]): Unit ={
//        //初始化spark
//        val sparkConf = new SparkConf()
//        sparkConf.setMaster("spark://" + host + ":7077")
//          .setAppName("HbaseToHive")
//          .set("spark.executor.memory", "2000m")
//          //          .setJars(List("target\\gapDetection-1.0-SNAPSHOT.jar"))
//          .setJars(new File(".\\lib\\jars\\")
//            .listFiles().map(_.getName).filter(_.split("\\.").last == "jar").map("lib\\jars\\" + _)
//            :+ "target\\workspace_jd_j-1.0-SNAPSHOT.jar")
//        val sess = SparkSession.builder().config(sparkConf).enableHiveSupport().getOrCreate()
//
//        //得到数据并转化
//        val hbaseConf = HBaseConfiguration.create()
//        hbaseConf.set("hbase.zookeeper.quorum", host) //设置zooKeeper集群地址
//        hbaseConf.set("hbase.zookeeper.property.clientPort", "2181") //设置zookeeper连接端口，默认2181
//        hbaseConf.set(TableInputFormat.INPUT_TABLE, tableName)
//        val HbaseData = sess.sparkContext.newAPIHadoopRDD(hbaseConf,classOf[TableInputFormat],classOf[ImmutableBytesWritable],classOf[Result])
//        val HbaseDataRDD = HbaseData.map(line => {
//          val hbaseCol = new ArrayBuffer[String]()
//          hbaseCol.append(Bytes.toString(line._2.getRow))
//          hbaseColName.foreach(name => hbaseCol.append(Bytes.toString(line._2.getValue(Bytes.toBytes(name.split(":")(0))
//            , Bytes.toBytes(name.split(":")(1))))))
//          Row.fromSeq(hbaseCol)
//        }).collect()
//    //    val schema = StructType(hiveColName.map(name => {StructField(name, StringType, nullable = true)}))
//    //    val hbaseDataDF = sess.createDataFrame(HbaseDataRDD, schema)
//    //    hbaseDataDF.show()
//        println()
//  }

  // 导出hbase的数据为csv文件，存在项目的target目录下
  def HbaseToCsv(host:String, hbasePort:String, tableName:String, HbaseColName: Array[String]): Unit ={
    //初始化hbase
    val writer = new CsvWriter("target/" + tableName + ".csv",',', Charset.forName("UTF-8"))
    val hbaseConf = HBaseConfiguration.create()
    hbaseConf.set("hbase.zookeeper.quorum", host) //设置zooKeeper集群地址
    hbaseConf.set("hbase.zookeeper.property.clientPort", hbasePort) //设置zookeeper连接端口，默认2181
    hbaseConf.set(TableInputFormat.INPUT_TABLE, tableName)
    val hbaseConnection = ConnectionFactory.createConnection(hbaseConf)
    val table = hbaseConnection.getTable(TableName.valueOf(tableName))
    val result = table.getScanner(new Scan())
    result.forEach(res =>{
      if (res.getValue(Bytes.toBytes("comment"),Bytes.toBytes("fin")) == null) { // 过滤结束符
        val value = HbaseColName.map(name => Bytes.toString(res.getValue(Bytes.toBytes(name.split(":")(0)), Bytes.toBytes(name.split(":")(1)))))
          .map(name => if (name != null) name.replaceAll("[\r\n,]", "，") else name) // 为了hive表导入，需要去除所有的小逗号
        val append = Bytes.toString(res.getRow) +: value
        writer.writeRecord(append)
      }
    })
    writer.close()

  }

  def inputToHdfs(host:String, port:String, localPath:String, HdfsPath:String): Unit ={
    val opr = FileSystem.get(new URI("hdfs://" + host + ":" + port), new Configuration(), "hadoop")
    opr.copyFromLocalFile(new Path(localPath), new Path(HdfsPath))
  }

  def CsvToHive(tableName:String, HdfsPath:String, HiveColName:Array[String]): Unit ={
    val dao = SessionBuilder.getSession("hive").getMapper(classOf[hiveTableDao])
    if (dao.findTable(tableName).size() == 0){
      println("目标表不存在，创建表...")
      dao.createCsvTable(tableName, HiveColName)
    }
    dao.loadCsv(HdfsPath, tableName)
    Thread.sleep(2000)
    dao.findTable(tableName).size() match {
      case 0 => println("导入失败！")
      case _ => println("导入成功！")
    }

  }

}



object TableConversion{
  def main(args: Array[String]): Unit = {
    val reader = new PropertiesReader()
    val tc = new TableConversion

//    val hbaseC = Array("detail:name","list:phone_name","list:phone_price","detail:brand","list:phone_comment_cnt")
//    val hiveC = Array("id", "name", "search_name", "price","brand", "comment_num")
    val hbaseC =Array("comment:content","comment:score","comment:plusAvailable")
    val hiveC = Array("id", "content", "score", "plus_level")

//    tc.HbaseToCsv(reader.getValue("host.master"), reader.getValue("port.zookeeper"),"jd_phone_info", hbaseC)
//    tc.HbaseToCsv(reader.getValue("host.master"), reader.getValue("port.zookeeper"),"jd_phone_comment", hbaseC)

//    tc.inputToHdfs(reader.getValue("host.master"), reader.getValue("port.hdfs"), "target/jd_phone_info.csv", "/test/")
//    tc.inputToHdfs(reader.getValue("host.master"), reader.getValue("port.hdfs"), "target/jd_phone_comment.csv", "/test/")

//    tc.CsvToHive("jd_phone_list", "/test/jd_phone_info.csv", hiveC)
    tc.CsvToHive("jd_phone_comment", "/test/jd_phone_comment.csv", hiveC)

  }
}
