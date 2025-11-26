# NebulaGraph Spark Connector

## Introduction

This repository is the NebulaGraph Connector for Apache Spark.

## Building for Spark2.4 (Optional)

1. Package NebulaGraph Spark Connector.

    ```bash
    $ git clone https://github.com/vesoft-inc/nebula-ng-tools.git
    $ cd spark-connector
    ```

   package spark-connector for spark2.4
    ```bash
    $ ./mvnw clean package -Dmaven.test.skip=true -Dgpg.skip -Dmaven.javadoc.skip=true  -pl spark2.4 -am -Pscala-2.11
    ```
   
    if you want to use assembly package for spark2.4, such as `spark-shell --jars`, please package the assembly jar:
    ```
    $ ./mvnw clean install -Dmaven.javadoc.skip=true -Dmaven.test.skip=true -pl assembly -am -Pscala-2.11 -Pspark2.4
    ```
    package spark-connector for spark3
    ```bash
    $ ./mvnw clean package -Dmaven.test.skip=true -Dgpg.skip -Dmaven.javadoc.skip=true  -pl spark3 -am -Pscala-2.12
    ```
   
    if you want to use assembly package for spark3, such as `spark-shell --jars`, please package the assembly jar:
    ```
    $ ./mvnw clean install -Dmaven.javadoc.skip=true -Dmaven.test.skip=true -pl assembly -am -Pscala-2.12 -Pspark3
    ```

   These commands will generate the corresponding targets:
   ```agsl
     spark-connector/spark2.4/target/nebula-connector_spark2.4-5.2-SNAPSHOT.jar
   ```
   ```agsl
     spark-connector/spark3/target/nebula-connector_spark3-5.2-SNAPSHOT.jar
   ```
   
## Integration with Apache Spark Applications

* Import nebula spark connector for spark2.4 with maven
  In your pom.xml,(change the artifactId to nebula-connector_spark3 for spark3) add:
  ```agsl
  <dependency>
     <groupId>com.vesoft</groupId>
     <artifactId>nebula-connector_spark2.4</artifactId>
     <version>5.2-SNAPSHOT</version>
  </dependency>
  ```
* Write DataFrame into NebulaGraph as Nodes:
  ```agsl
    val df = spark.read.json("spark-connector/example/src/main/resources/vertex")
    df.show()

    val nebulaConnectionConfig = NebulaConnectionConfig
      .builder()
      .withGraphAddress("192.168.8.6:3820")
      .withUser("root")
      .withPasswd("Nebula123")
      .build()
    val nebulaWriteNodeConfig: WriteNebulaNodeConfig = WriteNebulaNodeConfig
      .builder()
      .withGraphName("nba")
      .withNodeType("node_type_player")
      .withWriteMode(WriteMode.INSERT)
      .withBatchSize(10)
      .build()
    df.write.nebula(nebulaConnectionConfig, nebulaWriteNodeConfig).writeVertices()
  ```
  * Write DataFrame into NebulaGraph as Edges:
  ```agsl
    val df = spark.read.json("spark-connector/example/src/main/resources/edge")
    df.show()

    val nebulaWriteEdgeConfig: WriteNebulaEdgeConfig = WriteNebulaEdgeConfig
      .builder()
      .withGraphName("nba")
      .withEdge("edge_type_follow")
      .withSrcPkFields(List("src", "name1"))
      .withDstPkFields(List("dst", "name2"))
      .withSrcPksAsProperty(false)
      .withDstPksAsProperty(false)
      .withWriteMode(WriteMode.INSERTIGNORE)
      .withBatchSize(10)
      .build()
    df.write.nebula(getNebulaConnectionConfig, nebulaWriteEdgeConfig).writeEdges()
  ```

* Read DataFrame from NebulaGraph Node type:
```agsl
    val nebulaConnectionConfig = NebulaConnectionConfig
      .builder()
      .withGraphAddress("192.168.8.6:3820")
      .withUser("root")
      .withPasswd("Nebula123")
      .build()
    val nebulaNodeReadConfig: ReadNebulaConfig = ReadNebulaConfig
      .builder()
      .withGraphName("nba")
      .withTypeName("node_type_player")
      .withReturnCols(List("name"))
      .withBatchSize(10)
      .withPartitionNum(10)
      .build()
    val df = spark.read.nebula(nebulaConnectionConfig, nebulaNodeReadConfig).loadNode()
    df.show()
  ```

* Read DataFrame from NebulaGraph Edge type:
  ```agsl
    val nebulaConnectionConfig = NebulaConnectionConfig
      .builder()
      .withGraphAddress("192.168.8.6:3820")
      .withUser("root")
      .withPasswd("Nebula123")
      .build()
    val nebulaReadEdgeConfig: ReadNebulaConfig = ReadNebulaConfig
      .builder()
      .withGraphName("nba")
      .withTypeName("edge_type_follow")
      .withReturnCols(List("likeness"))
      .withBatchSize(1000)
      .withPartitionNum(10)
      .build()
    val df = spark.read.nebula(getNebulaConnectionConfig, nebulaReadEdgeConfig).loadEdge()
    df.show()
  ```
  


for complete example, see https://github.com/vesoft-inc/nebula-ng-tools/tree/master/spark-connector/example/src/main/scala/com/vesoft/nebula/example

for more configs, see https://github.com/vesoft-inc/nebula-ng-tools/blob/master/spark-connector/common/src/main/scala/com/vesoft/nebula/spark/common/NebulaConfig.scala
