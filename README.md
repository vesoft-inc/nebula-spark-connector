# Nebula Spark Connector
[中文版](https://github.com/vesoft-inc/nebula-spark-connector/blob/master/README_CN.md)

## Introduction

Nebula Spark Connector 2.0/3.0 only supports Nebula Graph 2.x/3.x. If you are using Nebula Graph v1.x, please use [Nebula Spark Connector v1.0](https://github.com/vesoft-inc/nebula-java/tree/v1.0/tools/nebula-spark) .

## How to Compile

1. Package Nebula Spark Connector.

    ```bash
    $ git clone https://github.com/vesoft-inc/nebula-spark-connector.git
    $ cd nebula-spark-connector/nebula-spark-connector
    $ mvn clean package -Dmaven.test.skip=true -Dgpg.skip -Dmaven.javadoc.skip=true
    ```

    After the packaging, you can see the newly generated nebula-spark-connector-3.0-SNAPSHOT.jar under the nebula-spark-connector/nebula-spark-connector/target/ directory.

## New Features (Compared to Nebula Spark Connector 1.0)
* Supports more connection configurations, such as timeout, connectionRetry, and executionRetry.
* Supports more data configurations, such as whether vertexId can be written as vertex's property, whether srcId, dstId and rank can be written as edge's properties.
* Spark Reader Supports non-property, all-property, and specific-properties read.
* Spark Reader Supports reading data from Nebula Graph to Graphx as VertexRD and EdgeRDD, it also supports String type vertexId.
* Nebula Spark Connector 2.0 uniformly uses SparkSQL's DataSourceV2 for data source expansion.
* Nebula Spark Connector 2.1.0 support UPDATE write mode to NebulaGraph, see [Update Vertex](https://docs.nebula-graph.io/2.0.1/3.ngql-guide/12.vertex-statements/2.update-vertex/) .
* Nebula Spark Connector 2.5.0 support DELETE write mode to NebulaGraph, see [Delete Vertex](https://docs.nebula-graph.io/master/3.ngql-guide/12.vertex-statements/4.delete-vertex/)

## How to Use
  
  If you use Maven to manage your project, add the following dependency to your pom.xml:
  ```
  <dependency>
     <groupId>com.vesoft</groupId>
     <artifactId>nebula-spark-connector</artifactId>
     <version>3.0-SNAPSHOT</version>
  </dependency>
  ```
  
  Write DataFrame `INSERT` into Nebula Graph as Vertices:
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
  Write DataFrame `UPDATE` into Nebula Graph as Vertices:
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
  Write DataFrame `DELETE` into Nebula Graph as Vertices:
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
  Read vertices from Nebula Graph: 
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

  Read vertices and edges from Nebula Graph to construct Graphx's graph:
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

## Version match

There are the version correspondence between Nebula Spark Connector and Nebula:

| Nebula Spark Connector Version | Nebula Version |
|:------------------------------:|:--------------:|
|           2.0.0                |  2.0.0, 2.0.1  |
|           2.0.1                |  2.0.0, 2.0.1  |
|           2.1.0                |  2.0.0, 2.0.1  |
|           2.5.0                |  2.5.0, 2.5.1  |
|           2.5.1                |  2.5.0, 2.5.1  |
|           2.6.0                |  2.6.0, 2.6.1  |
|           2.6.1                |  2.6.0, 2.6.1  |
|           3.0.0                |    3.0, 3.1    |
|         3.0-SNAPSHOT           |     nightly    |

## Performance
We use LDBC dataset to test nebula-spark-connector's performance, here's the result.

* For reader

We choose tag Comment and edge REPLY_OF for space sf30 and sf100 to test the connector reader. 
And the application's resources are: standalone mode with three workers, 2G driver-memory, 
3 num-executors, 30G executor-memory and 20 executor-cores.
The ReadNebulaConfig has 2000 limit and 100 partitionNum, 
the same partition number with nebula space parts.


|data type|ldbc 6.712million with No Property| ldbc 22 million with No Property|ldbc  6.712million with All Property|ldbc 22million with All Property|
|:-------:|:--------------------------------:|:-------------------------------:|:----------------------------------:|:------------------------------:|
| vertex  |                 9.405s           |           64.611s               |               13.897s              |            57.417s             |
|  edge   |                10.798s           |           71.499s               |               10.244s              |            67.43s              |


* For writer

We choose ldbc comment.csv to write into tag Comment, choose comment_replyOf_post.csv and 
comment_replyOf_comment.csv to write into edge REPLY_OF. 
And the application's resources are: standalone mode with three workers, 2G driver-memory, 
3 num-executors, 30G executor-memory and 20 executor-cores.
The writeConfig has 2000 batch sizes, and the DataFrame has 60 partitions.


|data type|ldbc 6.712million with All Property| ldbc 22 million with All Property|
|:-------:|:--------------------------------:|:-------------------------------:|
| vertex  |                 66.578s          |           112.829s              |
|  edge   |                 39.138s          |           51.198s               |

> Note: ldbc edge for REPLY_OF has no property.


## How to Contribute

Nebula Spark Connector is a completely opensource project, opensource enthusiasts are welcome to participate in the following ways:

- Go to [Nebula Graph Forum](https://discuss.nebula-graph.com.cn/ "go to“Nebula Graph Forum") to discuss with other users. You can raise your own questions, help others' problems, share your thoughts.
- Write or improve documents.
- Submit code to add new features or fix bugs.
