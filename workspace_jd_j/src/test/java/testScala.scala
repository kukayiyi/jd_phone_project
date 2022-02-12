import com.kuka.jd.DataProcessing.util.PropertiesReader
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.hadoop.hbase.client.{ConnectionFactory, Get, Scan}
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.util.Bytes

object testScala {
  def main(args: Array[String]): Unit = {
//    val properties:PropertiesReader = new PropertiesReader()
//    println(properties.getValue("host.master"))
//    val dd = Array("1", "2", "3", "4")
//    val ss = dd.slice(0,100)
//    ss.foreach(println)

    val hbaseConf = HBaseConfiguration.create()
    hbaseConf.set("hbase.zookeeper.quorum", "localhost") //设置zooKeeper集群地址
    hbaseConf.set("hbase.zookeeper.property.clientPort", "2181") //设置zookeeper连接端口，默认2181
    hbaseConf.set(TableInputFormat.INPUT_TABLE, "jd_phone_comment")
    val hbaseConnection = ConnectionFactory.createConnection(hbaseConf)
    val table = hbaseConnection.getTable(TableName.valueOf("jd_phone_comment"))
    val result = table.get(new Get(Bytes.toBytes("100016592731x122")))
//    println(result)
    val ss = result.getValue(Bytes.toBytes("comment"),Bytes.toBytes("fin"))
    val dd = Bytes.toString(ss).replaceAll("[\r\n,]", "，")
    println(Bytes.toString(ss))
  }

}
