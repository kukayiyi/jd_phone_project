package com.kuka.jd.DataProcessing

import com.kuka.jd.DataProcessing.dao.{hiveTableDao, mysqlTableDao}
import com.kuka.jd.DataProcessing.util.{PropertiesReader, SessionBuilder, jb}
import org.apache.ibatis.session.SqlSession
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SaveMode, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

import java.io.File
import java.sql.{Connection, DriverManager, ResultSet, Statement}
import java.util
import java.util.{List, Properties}
import scala.collection.JavaConverters.{collectionAsScalaIterableConverter, seqAsJavaListConverter}
import scala.collection.mutable.ArrayBuffer

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
    val sparkConf = new SparkConf()
    sparkConf.setJars(Array("target/workspace_jd_j-1.0-SNAPSHOT.jar", "lib/jars/mysql-connector-java-8.0.27.jar"))
      .set("spark.executor.memory", "2000m")
      .set("spark.default.parallelism", "100")
    this.sparkSession = SparkSession.builder()
      .appName("jd_exec")
      .master("spark://" + host + ":" + sparkPort)
//      .config("spark.jars", "lib/jars/mysql-connector-java-8.0.27.jar")
      .config(sparkConf)
      .enableHiveSupport()
      .getOrCreate()

  }

  // 统计不同品牌的商品数量
  def figureBrandNum(): Unit ={
    val df = this.sparkSession.sql("select lower(brand),count(*) as num from jd_phone_list group by lower(brand)")
    df.show()
    saveToMysql(df, "brand_num")
  }

  // 统计不同品牌的评论总数
  def figureCommentNumByBrand(): Unit ={
    val df = this.sparkSession.sql("select lower(brand),sum(comment) as sum from " +
      "(select brand,cast(comm as int) as comment from " +
      "(select replace(com,'万','0000') as comm,brand from " +
      "(select replace(comment_num,'+','') as com,brand from jd_phone_list))) " +
      "group by lower(brand)")
    df.show()
    saveToMysql(df, "com_num")

  }

  // 统计每个商品的平均分和评分人数
  def figureAvgScore(): Unit ={
    val df = this.sparkSession.sql("select a.pid,a.avg_score,a.total_num,b.name from " +
      "(select pid,avg(cast(score as int)) as avg_score,count(*) as total_num from " +
      "(select split(id,'x')[0] as pid,score from jd_phone_comment) group by pid) a " +
      "join jd_phone_list b " +
      "on(a.pid=b.id)")
    df.show()
    saveToMysql(df, "avg_score")

  }

  // 统计每个商品的评论中的前20个热词，用jieba实现
  def figureHotWord(): Unit ={
    val sc = this.sparkSession.sparkContext
    val filter = Array("好","错","手机","非常","其他","京东","华为","可以","特色",
      "小米","特别","支持","荣耀","使","苹果","高","喜欢","感觉",
      "棒","超级","起来","款","问题","较","长","来","着","屏",
      "能","拍","值得","上","适合","而且","麒麟","之前","要",
      "指纹","合适","体验","?","得","天",";","完美","已经",
      "会","太","人","再","价高","觉得","杠杠","完全")
    val result = this.sparkSession.sql("select split(id,'x')[0],content from jd_phone_comment where isnotnull(content) and content!=''").rdd  //取id和评论内容并转为rdd
      .map(row => (row.getString(0),jb.analysis(row.getString(1)).asScala.toList))  // 此时的结构是 RDD[(String, List[String])]
      .reduceByKey(_++_).collect()  // 将相同pid的评论词合在一起，并变成数组拉回本地，此时结构是 Array[(String, List[String])]
      .map(row => Array(row._1, sc.parallelize(row._2, math.max(row._2.length/100, 8))  // 开始对词进行处理，先转化为rdd，注意处理分区数
        .map((_,1)).reduceByKey(_+_)  // WordCount
        .sortBy(_._2, ascending = false)  // 排序
        .collect()  // 转化为数组
        .filter(word => !filter.contains(word._1))  // 过滤不需要的词
        .map(word => word._1 + ":" + word._2)   // 将词的元组转化为词:次数的字符串
        .slice(0, 20)   // 过滤前20个
        .mkString("，")))   // 并拼接成一个字符串
      .map(row => Row.fromSeq(row)).toList.asJava   // 存储转化操作，需要转化成util.List[Row](也就是java的list)的格式
    val schema = StructType(Array(StructField("pid", StringType, nullable = false), StructField("hot_word", StringType, nullable = true)))  // 这是字段描述类
    val df = this.sparkSession.createDataFrame(result, schema)
    df.show()
    saveToMysql(df, "hot_word")

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
//    pro.figureBrandNum()
//    pro.figureCommentNumByBrand()
//    pro.figureAvgScore()
    pro.figureHotWord()


  }

}
