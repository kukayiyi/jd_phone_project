package com.kuka.jd.DataProcessing

import com.kuka.jd.DataProcessing.dao.{hiveTableDao, mysqlTableDao}
import com.kuka.jd.DataProcessing.util.{PropertiesReader, SessionBuilder}
import org.apache.ibatis.session.SqlSession
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

import java.io.File
import java.sql.{Connection, DriverManager, ResultSet, Statement}
import java.util
import java.util.{List, Properties}

class processingMain{
  var hiveTableDao: hiveTableDao = _
  var sparkSession:SparkSession = _
  def this(host:String, sparkPort:String){
    this
    // 创建hive对于hbase的外部表
    this.hiveTableDao = SessionBuilder.getSession("hive").getMapper(classOf[hiveTableDao])
    if (this.hiveTableDao.findTable("jd_phone_list").size() == 0) this.hiveTableDao.createListTable()
//    if (this.hiveTableDao.findTable("jd_phone_comment").size() == 0) this.hiveTableDao.createCommentTable()

    //初始化spark
    this.sparkSession = SparkSession.builder()
      .appName("jd_exec")
      .master("spark://" + host + ":" + sparkPort)
      .config("spark.jars", "lib/jars/mysql-connector-java-8.0.27.jar")
      .enableHiveSupport()
      .getOrCreate()

  }

  // 统计不同品牌的商品数量
  def figureBrandNum(): Unit ={
    val df = this.sparkSession.sql("select brand,count(*) as num from jd_phone_list group by brand")
    df.show()
    saveToMysql(df, "brand_num")
  }

  // 统计不同品牌的评论总数
  def figureCommentNumByBrand(): Unit ={
    val df = this.sparkSession.sql("select brand,sum(comment) as sum from " +
      "(select brand,cast(comm as int) as comment from " +
      "(select replace(com,'万','0000') as comm,brand from " +
      "(select replace(comment_num,'+','') as com,brand from jd_phone_list))) " +
      "group by brand")
    df.show()
    saveToMysql(df, "com_num")

  }




  def saveToMysql(df:DataFrame, tableName:String): Unit ={
    val prop = new Properties()
    val reader = new PropertiesReader()
    val url = reader.getValue("mysql.url")
    prop.setProperty("user", reader.getValue("mysql.user"))
    prop.setProperty("password", reader.getValue("mysql.password"))
    prop.setProperty("driver", reader.getValue("mysql.driver"))
    prop.setProperty("url", url)
    df.write.mode(SaveMode.Overwrite).jdbc(url, tableName, prop)

  }





}

object processingMain {
  def main(args: Array[String]): Unit = {
    val properties = new PropertiesReader()
    val host = properties.getValue("host.master")
    val sparkPort = properties.getValue("port.spark")
    val pro = new processingMain(host, sparkPort)
    pro.figureBrandNum()
//    pro.figureCommentNumByBrand()


  }

}
