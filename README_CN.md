# 欢迎使用 Nebula Spark Connector
[English](https://github.com/vesoft-inc/nebula-spark-connector/blob/master/README.md)
## 介绍

Nebula Spark Connector 2.0/3.0 仅支持 Nebula Graph 2.x/3.x。如果您正在使用 Nebula Graph v1.x，请使用 [Nebula Spark Connector v1.0](https://github.com/vesoft-inc/nebula-java/tree/v1.0/tools)。

## 如何编译

1. 编译打包 Nebula Spark Connector。

    ```bash
    $ git clone https://github.com/vesoft-inc/nebula-spark-connector.git
    $ cd nebula-spark-connector/nebula-spark-connector
    $ mvn clean package -Dmaven.test.skip=true -Dgpg.skip -Dmaven.javadoc.skip=true
    ```

    编译打包完成后，可以在 nebula-spark-connector/nebula-spark-connector/target/ 目录下看到 nebula-spark-connector-3.0-SNAPSHOT.jar 文件。

## 特性

* 提供了更多连接配置项，如超时时间、连接重试次数、执行重试次数
* 提供了更多数据配置项，如写入数据时是否将 vertexId 同时作为属性写入、是否将 srcId、dstId、rank 等同时作为属性写入
* Spark Reader 支持无属性读取，支持全属性读取
* Spark Reader 支持将 Nebula Graph 数据读取成 Graphx 的 VertexRD 和 EdgeRDD，支持非 Long 型 vertexId
* Nebula Spark Connector 2.0 统一了 SparkSQL 的扩展数据源，统一采用 DataSourceV2 进行 Nebula Graph 数据扩展
* Nebula Spark Connector 2.1.0 增加了 UPDATE 写入模式，相关说明参考[Update Vertex](https://docs.nebula-graph.com.cn/2.0.1/3.ngql-guide/12.vertex-statements/2.update-vertex/)
* Nebula Spark Connector 2.5.0 增加了 DELETE 写入模式，相关说明参考[Delete Vertex](https://docs.nebula-graph.com.cn/2.5.1/3.ngql-guide/12.vertex-statements/4.delete-vertex/)

## 使用说明
  如果你使用Maven管理项目，请在pom.xml文件中增加依赖:
  ```
  <dependency>
     <groupId>com.vesoft</groupId>
     <artifactId>nebula-spark-connector</artifactId>
     <version>3.0-SNAPSHOT</version>
  </dependency>
  ```
  
  将 DataFrame 作为点 `INSERT` 写入 Nebula Graph :
  ```
    val config = NebulaConnectionConfig
      .builder()
      .withMetaAddress("127.0.0.1:9559")
      .withGraphAddress("127.0.0.1:9669")
      .build()
    val nebulaWriteVertexConfig = WriteNebulaVertexConfig
      .builder()
      .withSpace("test")
      .withTag("person")
      .withVidField("id")
      .withVidAsProp(true)
      .withBatch(1000)
      .build()
    df.write.nebula(config, nebulaWriteVertexConfig).writeVertices()
  ```
  将 DataFrame 作为点 `UPDATE` 写入 Nebula Graph :
  ```
    val config = NebulaConnectionConfig
      .builder()
      .withMetaAddress("127.0.0.1:9559")
      .withGraphAddress("127.0.0.1:9669")
      .build()
    val nebulaWriteVertexConfig = WriteNebulaVertexConfig
      .builder()
      .withSpace("test")
      .withTag("person")
      .withVidField("id")
      .withVidAsProp(true)
      .withBatch(1000)
      .withWriteMode(WriteMode.UPDATE)
      .build()
    df.write.nebula(config, nebulaWriteVertexConfig).writeVertices()
  ```
将 DataFrame 作为点 `DELETE` 写入 Nebula Graph :
  ```
    val config = NebulaConnectionConfig
      .builder()
      .withMetaAddress("127.0.0.1:9559")
      .withGraphAddress("127.0.0.1:9669")
      .build()
    val nebulaWriteVertexConfig = WriteNebulaVertexConfig
      .builder()
      .withSpace("test")
      .withTag("person")
      .withVidField("id")
      .withBatch(1000)
      .withWriteMode(WriteMode.DELETE)
      .build()
    df.write.nebula(config, nebulaWriteVertexConfig).writeVertices()
  ```

  读取 Nebula Graph 的点数据: 
  ```
    val config = NebulaConnectionConfig
      .builder()
      .withMetaAddress("127.0.0.1:9559")
      .withConenctionRetry(2)
      .build()
    val nebulaReadVertexConfig = ReadNebulaConfig
      .builder()
      .withSpace("exchange")
      .withLabel("person")
      .withNoColumn(false)
      .withReturnCols(List("birthday"))
      .withLimit(10)
      .withPartitionNum(10)
      .build()
    val vertex = spark.read.nebula(config, nebulaReadVertexConfig).loadVerticesToDF()
  ```

  读取 Nebula Graph 的点边数据构造 Graphx 的图：
  ```
    val config = NebulaConnectionConfig
      .builder()
      .withMetaAddress("127.0.0.1:9559")
      .withConenctionRetry(2)
      .build()
    val nebulaReadVertexConfig = ReadNebulaConfig
      .builder()
      .withSpace("exchange")
      .withLabel("person")
      .withNoColumn(false)
      .withReturnCols(List("birthday"))
      .withLimit(10)
      .withPartitionNum(10)
      .build()

    val nebulaReadEdgeConfig = ReadNebulaConfig
      .builder()
      .withSpace("exchange")
      .withLabel("knows1")
      .withNoColumn(false)
      .withReturnCols(List("timep"))
      .withLimit(10)
      .withPartitionNum(10)
      .build()

    val vertex = spark.read.nebula(config, nebulaReadVertexConfig).loadVerticesToGraphx()
    val edgeRDD = spark.read.nebula(config, nebulaReadEdgeConfig).loadEdgesToGraphx()
    val graph = Graph(vertexRDD, edgeRDD)
  ```
  得到 Graphx 的 Graph 之后，可以根据 [Nebula-Algorithm](https://github.com/vesoft-inc/nebula-algorithm/tree/master/nebula-algorithm) 的示例在 Graphx 框架中进行算法开发。

更多使用示例请参考 [Example](https://github.com/vesoft-inc/nebula-spark-connector/tree/master/example/src/main/scala/com/vesoft/nebula/examples/connector) 。

## PySpark 中使用 Nebula Spark Connector

### PySpark 中读取 NebulaGraph 中数据

从 `metaAddress` 为 `"metad0:9559"` 的 Nebula Graph 中读取整个 tag 下的数据为一个 dataframe：

```python
df = spark.read.format(
  "com.vesoft.nebula.connector.NebulaDataSource").option(
    "type", "vertex").option(
    "spaceName", "basketballplayer").option(
    "label", "player").option(
    "returnCols", "name,age").option(
    "metaAddress", "metad0:9559").option(
    "partitionNumber", 1).load()
```

然后可以像这样 `show` 这个 dataframe：

```python
>>> df.show(n=2)
+---------+--------------+---+
|_vertexId|          name|age|
+---------+--------------+---+
|player105|   Danny Green| 31|
|player109|Tiago Splitter| 34|
+---------+--------------+---+
only showing top 2 rows
```

### PySpark 中写 NebulaGraph 中数据

再试一试写入数据的例子，默认不指定的情况下 `writeMode` 是 `insert`：
#### 写入点
```python
df.write.format("com.vesoft.nebula.connector.NebulaDataSource").option(
    "type", "vertex").option(
    "spaceName", "basketballplayer").option(
    "label", "player").option(
    "vidPolicy", "").option(
    "vertexField", "_vertexId").option(
    "batch", 1).option(
    "metaAddress", "metad0:9559").option(
    "graphAddress", "graphd1:9669").option(
    "passwd", "nebula").option(
    "user", "root").save()
```
#### 删除点
如果想指定 `delete` 或者 `update` 的非默认写入模式，增加 `writeMode` 的配置，比如 `delete` 的例子：

```python
df.write.format("com.vesoft.nebula.connector.NebulaDataSource").option(
    "type", "vertex").option(
    "spaceName", "basketballplayer").option(
    "label", "player").option(
    "vidPolicy", "").option(
    "vertexField", "_vertexId").option(
    "batch", 1).option(
    "metaAddress", "metad0:9559").option(
    "graphAddress", "graphd1:9669").option(
    "passwd", "nebula").option(
    "writeMode", "delete").option(
    "user", "root").save()
```
#### 写入边
```python
df.write.format("com.vesoft.nebula.connector.NebulaDataSource")\
    .mode("overwrite")\
    .option("srcPolicy", "")\
    .option("dstPolicy", "")\
    .option("metaAddress", "metad0:9559")\
    .option("graphAddress", "graphd:9669")\
    .option("user", "root")\
    .option("passwd", "nebula")\
    .option("type", "edge")\
    .option("spaceName", "basketballplayer")\
    .option("label", "server")\
    .option("srcVertexField", "srcid")\
    .option("dstVertexField", "dstid")\
    .option("rankFiled", "")\
    .option("batch", 100)\
    .option("writeMode", "insert").save()   # delete to delete edge, update to update edge
```
#### 删除边
```python
df.write.format("com.vesoft.nebula.connector.NebulaDataSource")\
    .mode("overwrite")\
    .option("srcPolicy", "")\
    .option("dstPolicy", "")\
    .option("metaAddress", "metad0:9559")\
    .option("graphAddress", "graphd:9669")\
    .option("user", "root")\
    .option("passwd", "nebula")\
    .option("type", "edge")\
    .option("spaceName", "basketballplayer")\
    .option("label", "server")\
    .option("srcVertexField", "srcid")\
    .option("dstVertexField", "dstid")\
    .option("rankFiled", "")\
    .option("batch", 100)\
    .option("writeMode", "delete").save()   # delete to delete edge, update to update edge
```

### 关于 PySpark 读写的 option


对于其他的 option，比如删除点的时候的 `withDeleteEdge` 可以参考 [nebula/connector/NebulaOptions.scala
](https://github.com/vesoft-inc/nebula-spark-connector/blob/master/nebula-spark-connector/src/main/scala/com/vesoft/nebula/connector/NebulaOptions.scala) 的字符串配置定义，我们可以看到它的字符串定义字段是 `deleteEdge` ：

```scala
  /** write config */
  val RATE_LIMIT: String   = "rateLimit"
  val VID_POLICY: String   = "vidPolicy"
  val SRC_POLICY: String   = "srcPolicy"
  val DST_POLICY: String   = "dstPolicy"
  val VERTEX_FIELD         = "vertexField"
  val SRC_VERTEX_FIELD     = "srcVertexField"
  val DST_VERTEX_FIELD     = "dstVertexField"
  val RANK_FIELD           = "rankFiled"
  val BATCH: String        = "batch"
  val VID_AS_PROP: String  = "vidAsProp"
  val SRC_AS_PROP: String  = "srcAsProp"
  val DST_AS_PROP: String  = "dstAsProp"
  val RANK_AS_PROP: String = "rankAsProp"
  val WRITE_MODE: String   = "writeMode"
  val DELETE_EDGE: String  = "deleteEdge"
```

### 如何在 PySpark 中调用 Nebula Spark Connector

最后，这里给出用 PySpark Shell 和在 Python 代码里调用 Spark Connector 的例子：

- Call with PySpark shell:

```bash
/spark/bin/pyspark --driver-class-path nebula-spark-connector-3.0.0.jar --jars nebula-spark-connector-3.0.0.jar
```

- In Python code:

```
from pyspark.sql import SparkSession

spark = SparkSession.builder.config(
    "nebula-spark-connector-3.0.0.jar",
    "/path_to/nebula-spark-connector-3.0.0.jar").appName(
        "nebula-connector").getOrCreate()

# read vertex
df = spark.read.format(
  "com.vesoft.nebula.connector.NebulaDataSource").option(
    "type", "vertex").option(
    "spaceName", "basketballplayer").option(
    "label", "player").option(
    "returnCols", "name,age").option(
    "metaAddress", "metad0:9559").option(
    "partitionNumber", 1).load()
```

## 版本匹配
Nebula Spark Connector 和 Nebula 的版本对应关系如下:

| Nebula Spark Connector Version | Nebula Version |
|:------------------------------:|:--------------:|
|          2.0.0                 |  2.0.0, 2.0.1  |
|          2.0.1                 |  2.0.0, 2.0.1  |
|          2.1.0                 |  2.0.0, 2.0.1  |
|          2.5.0                 |  2.5.0, 2.5.1  |
|          2.5.1                 |  2.5.0, 2.5.1  |
|          2.6.0                 |  2.6.0, 2.6.1  |
|          2.6.1                 |  2.6.0, 2.6.1  |
|          3.0.0                 |     3.0.0      |
|        3.0-SNAPSHOT            |     nightly    |

## 性能
我们使用LDBC数据集进行Nebula-Spark-Connector的性能测试，测试结果如下：

* reader

我们选择LDBC导入Nebula Space sf30 和 sf100 之后的 Comment 标签和 REPLY_OF 边类型进行数据读取。
其中读取应用程序的资源配置为：提交模式为具有三个工作节点的Spark standalone模式，2G driver-memory, 
3 num-executors， 30G executor-memory，20 executor-cores。
读取Nebula数据的配置是 2000 limit 和 100 partitionNum，其中 space 的 分区数也是100。


|data type|ldbc 6.712million with No Property| ldbc 22 million with No Property|ldbc  6.712million with All Property|ldbc 22million with All Property|
|:-------:|:--------------------------------:|:-------------------------------:|:----------------------------------:|:------------------------------:|
| vertex  |                 9.405s           |           64.611s               |               13.897s              |            57.417s             |
|  edge   |                10.798s           |           71.499s               |               10.244s              |            67.43s              |


* writer

我们选择 LDBC sf30 和 sf100 数据集中的 comment.csv 写入 Comment 标签， 选择 LDBC sf30 和 sf100 数据集中的 
comment_replyOf_post.csv and comment_replyOf_comment.csv 写入 REPLY_OF 边类型。
其中写入应用程序的资源配置为：提交模式为具有三个工作节点的Spark standalone模式，2G driver-memory, 
3 num-executors， 30G executor-memory，20 executor-cores。
写入Nebula的配置是 2000 batch size， 待写入的数据 DataFrame 有 60 个 Spark 分区。


|data type|ldbc 6.712million with All Property| ldbc 22 million with All Property|
|:-------:|:--------------------------------:|:-------------------------------:|
| vertex  |                 66.578s          |           112.829s              |
|  edge   |                 39.138s          |           51.198s               |

> 注意: LDBC 的 REPLY_OF 边数据无属性。


## 贡献

Nebula Spark Connector 是一个完全开源的项目，欢迎开源爱好者通过以下方式参与：

- 前往 [Nebula Graph 论坛](https://discuss.nebula-graph.com.cn/ "点击前往“Nebula Graph 论坛") 上参与 Issue 讨论，如答疑、提供想法或者报告无法解决的问题
- 撰写或改进文档
- 提交优化代码
