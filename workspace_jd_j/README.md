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

