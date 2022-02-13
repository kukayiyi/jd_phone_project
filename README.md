# jd_phone_project
京东手机数据分析项目

# 说明：

​	本项目是根据@达瓦里氏 的“京东手机电商大数据统计平台”项目（原项目地址[京东手机电商大数据统计平台: 一个电商大数据统计系统，对消京东平台的手机热点进行统计分析 (gitee.com)](https://gitee.com/wu_handsome_man/JDPhone)）扩充而成。原项目为浪潮培训课设项目，功能上较为淡薄，出于测试分布式工具和学习的目的，本项目对原项目进行了扩充，在此感谢原项目作者！

​	更多说明分别在workspace_jd、workspace_jd_j两个目录下

### 项目功能

​	从京东手机商品页面爬取手机商品的简略信息、详细参数信息和评论评分信息，并使用分布式工具存储数据和对数据进行离线处理。目前实现的结果如下：

```
统计不同品牌的商品数量
统计不同品牌的评论总数
统计每个商品的平均分和评分人数
统计每个商品的评论中的前20个热词，用jieba实现
```

### 软件架构

​	使用Python爬取数据并使用kafka将数据生产到服务器，消费端从服务器拉取数据并将数据存入database。使用hbase存储全部数据，分析时导出需要的数据到hive表中并使用spark进行数据处理，结果存入mysql中。

### 软件版本

```
<spark.version>3.1.2</spark.version>
<hadoop.version>3.2.2</hadoop.version>
<zookeeper.version>3.6.3</zookeeper.version>
<hbase.version>2.4.6</hbase.version>
<zookeeper.version>3.6.3</zookeeper.version>
<kafka.version>2.8.0</kafka.version>
<hive.version>3.1.2</hive.version> （实际pom里是2.7.3，这个是兼容问题）
<mysql.version>8.0.27</mysql.version>
```

### 项目架构

/jd_phone_project																									总项目

​		/work_space_jd																								项目的Python部分（生产端）

​				/src/Get																									原项目的爬取脚本，可以用来测试

​				/src/KafkaProducter																				 本项目的爬取脚本和kafka生产代码

​						/jdPhoneProducer.py																	   ①爬取主脚本

​				/target																									  测试生成的csv文件

​		/work_space_jd_j																							项目的Java/Scala部分（消费端）

​				/lib																										   一些spark用的jar包

​				/src/main/resource																				  配置文件

​				/src/main/java/com/kuka																		 项目主体

​						/kafka/consumer																			 ②kafka消费类

​						/jd/DataProcessing																		 数据处理主体

​								/dao																					    mybatis的dao

​								/util																						 工具类

​								/TableConversion.scala														 ③数据导入导出类

​								/processingMain.scala														   ④spark sql数据处理类

​								

### 运行流程

1、使用①对数据进行爬取并生产

2、使用②消费数据

3、使用③将要分析的数据导出

4、使用④对数据进行分析处理

### 数据说明

​	项目一共爬取到商品数4438，包含属性：商品id,商品名,搜索名,详情页链接,价格,评论数,卖家名,是否自营,商品毛重,商品产地,CPU型号,运行内存,机身存储,摄像头数量,后摄主摄像素,前摄主摄像素,主屏幕尺寸（英寸）,分辨率,屏幕比例,充电器,热点,操作系统,游戏配置,品牌

​	项目一共爬取到评论数1529863,包含属性："序号", "guid", "评论内容", "评论时间", "参考id", "参考时间", "评分", "顾客会员等级", "是否手机", "购物使用的平台"

​	平台上的数据不断变化，经常有商品失效，因此复现时要重爬一遍，不过还是附上现有数据集。

### 相对原项目的改进

1、爬取方面，原项目爬取数量较少，为了爬取数据更多更快，增加了代理池的设置。

2、存储方面，原项目也许限于条件，并没有使用太多分布式组件，本项目由于数据量的增大，使用hbase和hive来存储。

3、数据处理上将需求和实现优化，兼顾了分布式组件的较全面的测试。

### 联系

本项目旨在测试和学习，bug和烂代码不可避免，如有修改意见，或者有更多需求建议，欢迎联系！

QQ&Email：986641660@qq.com



====================================一些笔记性质的附加说明========================================

# 关于Python部分的说明

​	最开始我是没想到会在爬取部分花费40%的时间的。原项目数据量较小，因此设置爬取间隔5-10秒时并不会被ban。而随着数据量的增大，不可避免的要和网站的反爬机制斗智斗勇。

目前我所了解的反反爬手段有三：

1、控制速度，最原始的方法，但不一定就不好用。是经过测试不同网站对此的容忍度不同，在我爬京东手机详情页的时候发现即便以10秒左右的间隔，爬200-400条后依然会被ban，而爬评论时就不会，因此要测试后合理选择。

2、更换user-agent，可以直接pip install fake_useragent来做到，但是这个一般是配合别的手段来，同一个ip不停的更换ua在网站管理者的角度来说是极容易检测的。

3、更换ip。最原始的方法当然是重新拨号获得新ip，但这显然不适合大批量爬取，于是一般只能去网上买代理ip的服务。我使用的蜻蜓代理，写这篇文章的时候是25一天(短效优质)，测试效果不是很好，但是横向对比价格比较便宜，见仁见智。一般代理池又主要提供两种服务：①隧道代理，即将请求直接发给服务商的代理池，他负责使用随机的ip访问请求并返回给你结果，这种一般便宜，但是限制的频率一般较低，爬取速度会慢。②ip代理，即需要你自己搭建代理池，代理池向服务商请求ip列表，你根据列表代理ip访问请求，我使用的代理池是https://github.com/jhao104/proxy_pool 。这种就完全看提供的ip质量，如果质量不好，比如我测试的蜻蜓代理一多半不能用，爬起来说不定还不如隧道快，但是如果ip质量好就能很快，因此也许你需要多家对比测试。

# 关于java部分（数据处理部分）的说明

使用时，先在src/resource/connection.properties中更改你的集群信息，然后根据主页的顺序跑。

### 运行方式

​	一般spark打包到集群上运行不利于调试，因此我都是直接在ide中运行，不过这会导致一些不便之处，如：

1、运行spark任务时driver端不能开代理，否则会卡住。

2、有可能会碰到莫名其妙的缺jar包的情况，且只能在编程中使用sparkconf.setjars来添加，在集群中添加无效（这个如果有人有解决方法请务必联系我）。

3、有时候报序列化失败错误，需要maven package再将打出的jar包用sparkconf.setjars加进去。注意把spark任务运行不需要的依赖去掉，否则也许会打出巨大的jar包，占内存。

### spark的调优

如果数据量大的任务跑的不顺利，在processingMain.scala类中改下面几个地方可能有帮助：

```
sparkConf.set("spark.executor.memory", "2000m")
```

改运行内存，根据实际条件来改，注意如果你spark-env.sh里配了运行内存上限的话不要超了。

```
sparkConf.set("spark.default.parallelism", "100")
```

默认分区数，也就是并行数，task数，也要根据数据量改，默认好像是根据核数来的，我是2。

```
sc.parallelize(row._2, math.max(row._2.length/50, 8)
```

转化数据集为RDD时设置的分区数，没研究透到底设多少好。

### 碎碎念组件冲突

实验测得组件冲突：

spark3与hive3冲突（spark-hive包截至3.2.1版本只支持到hive2.3.9）

企图更换hive3至hive2，发现hive2与hadoop3冲突（grava包版本冲突，hadoop3.2.2使用grava27，远超其他组件使用的grava十几）

后发现hive3也可以使用hive2的jar包进行基本通信（即spark可以读hive表），但spark无法与hive的hbase外部表通信（要用到hive-hbase-handler包，而此包的2.x版本只支持hbase1.x，想要支持hbase2.x需要此包的3.x版本，但此包和hive的基础jar包重合性很大，由于上面限制只能使用hive2的jar包，因此此包的3.x版本无法使用，陷入死循环）

因此预计这样一套环境才可以完全不冲突：hadoop2.x，spark2或3，hive2，zookeeper3，hbase1。由于数据关系我不能这么大换血了，有实验过的同志可以联系我。

