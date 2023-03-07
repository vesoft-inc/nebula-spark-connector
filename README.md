# NebulaGraph Spark Connector
[中文版](https://github.com/vesoft-inc/nebula-spark-connector/blob/master/README_CN.md)

## Introduction

NebulaGraph Spark Connector 2.0/3.0 only supports NebulaGraph 2.x/3.x. If you are using NebulaGraph v1.x, please use [NebulaGraph Spark Connector v1.0](https://github.com/vesoft-inc/nebula-java/tree/v1.0/tools/nebula-spark) .

NebulaGraph Spark Connector support spark 2.2 and 2.4.

## How to Compile

1. Package NebulaGraph Spark Connector.

    ```bash
    $ git clone https://github.com/vesoft-inc/nebula-spark-connector.git
    $ cd nebula-spark-connector/nebula-spark-connector
    $ mvn clean package -Dmaven.test.skip=true -Dgpg.skip -Dmaven.javadoc.skip=true
    ```
   if you want to use connector for spark 2.2.x, use the command:
   ```
   $ cd nebula-spark-connector/nebula-spark-connector_2.2
   $ mvn clean package -Dmaven.test.skip=true -Dgpg.skip -Dmaven.javadoc.skip=true
   ```

    After the packaging, you can see the newly generated nebula-spark-connector-3.0-SNAPSHOT.jar under the nebula-spark-connector/nebula-spark-connector/target/ directory.

## New Features (Compared to NebulaGraph Spark Connector 1.0)
* Supports more connection configurations, such as timeout, connectionRetry, and executionRetry.
* Supports more data configurations, such as whether vertexId can be written as vertex's property, whether srcId, dstId and rank can be written as edge's properties.
* Spark Reader Supports non-property, all-property, and specific-properties read.
* Spark Reader Supports reading data from NebulaGraph to Graphx as VertexRD and EdgeRDD, it also supports String type vertexId.
* NebulaGraph Spark Connector 2.0 uniformly uses SparkSQL's DataSourceV2 for data source expansion.
* NebulaGraph Spark Connector 2.1.0 support UPDATE write mode to NebulaGraph, see [Update Vertex](https://docs.nebula-graph.io/2.0.1/3.ngql-guide/12.vertex-statements/2.update-vertex/) .
* NebulaGraph Spark Connector 2.5.0 support DELETE write mode to NebulaGraph, see [Delete Vertex](https://docs.nebula-graph.io/master/3.ngql-guide/12.vertex-statements/4.delete-vertex/)

## How to Use
  
  If you use Maven to manage your project, add one of the following dependency to your pom.xml:
  
  ```
  <!-- connector for spark 2.4 -->
  <dependency>
     <groupId>com.vesoft</groupId>
     <artifactId>nebula-spark-connector</artifactId>
     <version>3.0-SNAPSHOT</version>
  </dependency>

 <!-- connector for spark 2.2 -->
  <dependency>
     <groupId>com.vesoft</groupId>
     <artifactId>nebula-spark-connector_2.2</artifactId>
     <version>3.0-SNAPSHOT</version>
  </dependency>

 <!-- connector for spark 3.0 -->
  <dependency>
     <groupId>com.vesoft</groupId>
     <artifactId>nebula-spark-connector_3.0</artifactId>
     <version>3.0-SNAPSHOT</version>
  </dependency>
  ```
  
  Write DataFrame `INSERT` into NebulaGraph as Vertices:
  ```
    val config = NebulaConnectionConfig
      .builder()
      .withMetaAddress("127.0.0.1:9559")
      .withGraphAddress("127.0.0.1:9669")
      .build()
    val nebulaWriteVertexConfig: WriteNebulaVertexConfig = WriteNebulaVertexConfig
      .builder()
      .withSpace("test")
      .withTag("person")
      .withVidField("id")
      .withVidAsProp(true)
      .withBatch(1000)
      .build()
    df.write.nebula(config, nebulaWriteVertexConfig).writeVertices()
  ```
  Write DataFrame `UPDATE` into NebulaGraph as Vertices:
  ```
    val config = NebulaConnectionConfig
      .builder()
      .withMetaAddress("127.0.0.1:9559")
      .withGraphAddress("127.0.0.1:9669")
      .build()
    val nebulaWriteVertexConfig: WriteNebulaVertexConfig = WriteNebulaVertexConfig
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
  Write DataFrame `DELETE` into NebulaGraph as Vertices:
  ```
    val config = NebulaConnectionConfig
      .builder()
      .withMetaAddress("127.0.0.1:9559")
      .withGraphAddress("127.0.0.1:9669")
      .build()
    val nebulaWriteVertexConfig: WriteNebulaVertexConfig = WriteNebulaVertexConfig
      .builder()
      .withSpace("test")
      .withTag("person")
      .withVidField("id")
      .withBatch(1000)
      .withWriteMode(WriteMode.DELETE)
      .build()
    df.write.nebula(config, nebulaWriteVertexConfig).writeVertices()
  ```
  Read vertices from NebulaGraph: 
  ```
    val config = NebulaConnectionConfig
      .builder()
      .withMetaAddress("127.0.0.1:9559")
      .withConenctionRetry(2)
      .build()
    val nebulaReadVertexConfig: ReadNebulaConfig = ReadNebulaConfig
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

  Read vertices and edges from NebulaGraph to construct Graphx's graph:
  ```
    val config = NebulaConnectionConfig
      .builder()
      .withMetaAddress("127.0.0.1:9559")
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
  After getting Graphx's Graph, you can develop graph algorithms in Graphx like [Nebula-Algorithm](https://github.com/vesoft-inc/nebula-algorithm/tree/master/nebula-algorithm).

For more information on usage, please refer to [Example](https://github.com/vesoft-inc/nebula-spark-connector/tree/master/example/src/main/scala/com/vesoft/nebula/examples/connector).

## PySpark with NebulaGraph Spark Connector

Below is an example of calling nebula-spark-connector jar package in pyspark.

### Read in PySpark

Read from NebulaGraph with `metaAddress` of `"metad0:9559"` as a dataframe:

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

You may then `show` the dataframe as follow:

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

### Write in PySpark

Let's try a write example, by default, the `writeMode` is `insert`
#### write vertex
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
#### delete vertex
For delete or update write mode, we could(for instance)specify with `writeMode` as `delete` like:
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
#### write edge
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
    .option("randkField", "")\
    .option("batch", 100)\
    .option("writeMode", "insert").save()   # delete to delete edge, update to update edge
```
#### delete edge
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
    .option("randkField", "")\
    .option("batch", 100)\
    .option("writeMode", "delete").save()   # delete to delete edge, update to update edge
```

### Options in PySpark

For more options, i.e. delete edge with vertex being deleted, refer to [nebula/connector/NebulaOptions.scala
](https://github.com/vesoft-inc/nebula-spark-connector/blob/master/nebula-spark-connector/src/main/scala/com/vesoft/nebula/connector/NebulaOptions.scala), we could know it's named as `deleteEdge` in option.

```scala
  /** write config */
  val RATE_LIMIT: String   = "rateLimit"
  val VID_POLICY: String   = "vidPolicy"
  val SRC_POLICY: String   = "srcPolicy"
  val DST_POLICY: String   = "dstPolicy"
  val VERTEX_FIELD         = "vertexField"
  val SRC_VERTEX_FIELD     = "srcVertexField"
  val DST_VERTEX_FIELD     = "dstVertexField"
  val RANK_FIELD           = "randkField"
  val BATCH: String        = "batch"
  val VID_AS_PROP: String  = "vidAsProp"
  val SRC_AS_PROP: String  = "srcAsProp"
  val DST_AS_PROP: String  = "dstAsProp"
  val RANK_AS_PROP: String = "rankAsProp"
  val WRITE_MODE: String   = "writeMode"
  val DELETE_EDGE: String  = "deleteEdge"
```

### Call NebulaGraph Spark Connector in PySpark shell and .py file

Also, below are examples on how we run above code with pyspark shell or in python code files:

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

## Compatibility matrix

There are the version correspondence between NebulaGraph Spark Connector and Nebula、Spark:

| NebulaGraph Spark Connector Version            | NebulaGraph Version | Spark Version |
|:-----------------------------------------:|:--------------:|:-------------:|
|nebula-spark-connector-2.0.0.jar           |  2.0.0, 2.0.1  | 2.4.* |
|nebula-spark-connector-2.0.1.jar           |  2.0.0, 2.0.1  | 2.4.* | 
|nebula-spark-connector-2.1.0.jar           |  2.0.0, 2.0.1  | 2.4.* |
|nebula-spark-connector-2.5.0.jar           |  2.5.0, 2.5.1  | 2.4.* |
|nebula-spark-connector-2.5.1.jar           |  2.5.0, 2.5.1  | 2.4.* |
|nebula-spark-connector-2.6.0.jar           |  2.6.0, 2.6.1  | 2.4.* |
|nebula-spark-connector-2.6.1.jar           |  2.6.0, 2.6.1  | 2.4.* |
|nebula-spark-connector-3.0.0.jar           |      3.x       | 2.4.* |
|nebula-spark-connector-3.3.0.jar           |      3.x       | 2.4.* |
|nebula-spark-connector_2.2-3.3.0.jar       |      3.x       | 2.2.* |
|nebula-spark-connector-3.4.0.jar           |      3.x       | 2.4.* |
|nebula-spark-connector_2.2-3.4.0.jar       |      3.x       | 2.2.* |
|nebula-spark-connector-3.0-SNAPSHOT.jar    |     nightly    | 2.4.* |
|nebula-spark-connector_2.2-3.0-SNAPSHOT.jar|     nightly    | 2.2.* |
|nebula-spark-connector_3.0-3.0-SNAPSHOT.jar|     nightly    | 3.*   |

## Performance
We use LDBC dataset to test nebula-spark-connector's performance, here's the result.

* For reader

We choose tag Comment and edge REPLY_OF for space sf30 and sf100 to test the connector reader. 
And the application's resources are: standalone mode with three workers, 2G driver-memory, 
3 num-executors, 30G executor-memory and 20 executor-cores.
The ReadNebulaConfig has 2000 limit and 100 partitionNum, 
the same partition number with NebulaGraph space parts.


|data type|ldbc 67.12million with No Property| ldbc 220 million with No Property|ldbc  67.12million with All Property|ldbc 220 million with All Property|
|:-------:|:--------------------------------:|:-------------------------------:|:----------------------------------:|:------------------------------:|
| vertex  |                 9.405s           |           64.611s               |               13.897s              |            57.417s             |
|  edge   |                10.798s           |           71.499s               |               10.244s              |            67.43s              |


* For writer

We choose ldbc comment.csv to write into tag Comment, choose comment_replyOf_post.csv and 
comment_replyOf_comment.csv to write into edge REPLY_OF. 
And the application's resources are: standalone mode with three workers, 2G driver-memory, 
3 num-executors, 30G executor-memory and 20 executor-cores.
The writeConfig has 2000 batch sizes, and the DataFrame has 60 partitions.


|data type|ldbc 67.12million with All Property| ldbc 220 million with All Property|
|:-------:|:--------------------------------:|:-------------------------------:|
| vertex  |                 66.578s          |           112.829s              |
|  edge   |                 39.138s          |           51.198s               |

> Note: ldbc edge for REPLY_OF has no property.


## How to Contribute

NebulaGraph Spark Connector is a completely opensource project, opensource enthusiasts are welcome to participate in the following ways:

- Go to [NebulaGraph Forum](https://discuss.nebula-graph.com.cn/ "go to“NebulaGraph Forum") to discuss with other users. You can raise your own questions, help others' problems, share your thoughts.
- Write or improve documents.
- Submit code to add new features or fix bugs.
