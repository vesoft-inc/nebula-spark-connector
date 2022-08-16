/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

package com.vesoft.nebula.examples.connector

import com.facebook.thrift.protocol.TCompactProtocol
import com.vesoft.nebula.connector.{
  NebulaConnectionConfig,
  WriteMode,
  WriteNebulaEdgeConfig,
  WriteNebulaVertexConfig
}
import com.vesoft.nebula.connector.connector.NebulaDataFrameWriter
import com.vesoft.nebula.connector.ssl.SSLSignType
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel
import org.slf4j.LoggerFactory

object NebulaSparkWriterExample {
  private val LOG = LoggerFactory.getLogger(this.getClass)

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf
    sparkConf
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.driver.allowMultipleContexts", "true")
      .registerKryoClasses(Array[Class[_]](classOf[TCompactProtocol]))
//      .set("spark.driver.allowMultipleContexts", "true")
//      .set("spark.neo4j.bolt.url", "bolt://192.168.8.87:7687/")
//      .set("spark.neo4j.bolt.user", "neo4j")
//      .set("spark.neo4j.bolt.password", "nebula")

    val spark = SparkSession
      .builder()
      .master("local")
      .config(sparkConf)
      .getOrCreate()

    import org.neo4j.spark._

    val sparkConfSettings = spark.sparkContext.getConf.getAll

    val newSparkConf = new SparkConf()
      .set("spark.driver.allowMultipleContexts", "true")
      .set("spark.neo4j.bolt.url", "bolt://192.168.8.87:7687/")
      .set("spark.neo4j.bolt.user", "neo4j")
      .set("spark.neo4j.bolt.password", "nebula")
    for (entry <- sparkConfSettings) {
      newSparkConf.set(entry._1, entry._2)
    }
    val sc  = new SparkContext(newSparkConf)
    val neo = Neo4j(sc)

    val data = neo
      .cypher("MATCH (n:对公客户) RETURN id(n) as id ")
      .partitions(1)
      .batch(25)
      .loadDataFrame
    data.show()

    //   => res36: Long = 100

    //val df = neo.pattern("对公客户", Seq("p"), "对公客户").partitions(12).batch(100).loadDataFrame

    val df = neo
      .cypher("match p=() -[r:p]->() return r.pop1, r.pop2, r.pop3, r.pop4")
      .partitions(1)
      .batch(10)
      .loadDataFrame
    df.show()
//    writeVertex(spark)
//    writeEdge(spark)
//
//    updateVertex(spark)
//    updateEdge(spark)
//
//    deleteVertex(spark)
//    deleteEdge(spark)

    spark.close()
  }

  def getNebulaConnectionConfig(): NebulaConnectionConfig = {
    // connection config without ssl
    val config =
      NebulaConnectionConfig
        .builder()
        .withMetaAddress("127.0.0.1:9559")
        .withGraphAddress("127.0.0.1:9669")
        .withConenctionRetry(2)
        .build()

    // connection config with ca ssl
    val configWithCaSsl =
      NebulaConnectionConfig
        .builder()
        .withMetaAddress("127.0.0.1:9559")
        .withGraphAddress("127.0.0.1:9669")
        .withConenctionRetry(2)
        .withEnableMetaSSL(true)
        .withEnableGraphSSL(true)
        .withSSLSignType(SSLSignType.CA)
        .withCaSSLSignParam("example/src/main/resources/ssl/casigned.pem",
                            "example/src/main/resources/ssl/casigned.crt",
                            "example/src/main/resources/ssl/casigned.key")
        .build()

    // connection config with self ssl
    val configWithSelfSsl =
      NebulaConnectionConfig
        .builder()
        .withMetaAddress("127.0.0.1:9559")
        .withGraphAddress("127.0.0.1:9669")
        .withConenctionRetry(2)
        .withEnableMetaSSL(true)
        .withEnableGraphSSL(true)
        .withSSLSignType(SSLSignType.SELF)
        .withSelfSSLSignParam("example/src/main/resources/ssl/selfsigned.pem",
                              "example/src/main/resources/ssl/selfsigned.key",
                              "vesoft")
        .build()

    config
  }

  /**
    * for this example, your nebula tag schema should have property names: name, age, born
    * if your withVidAsProp is true, then tag schema also should have property name: id
    */
  def writeVertex(spark: SparkSession): Unit = {
    LOG.info("start to write nebula vertices")
    val df = spark.read.json("example/src/main/resources/vertex")
    df.show()

    val config = getNebulaConnectionConfig()
    val nebulaWriteVertexConfig: WriteNebulaVertexConfig = WriteNebulaVertexConfig
      .builder()
      .withSpace("test")
      .withTag("person")
      .withVidField("id")
      .withVidAsProp(false)
      .withBatch(1000)
      .build()
    df.write.nebula(config, nebulaWriteVertexConfig).writeVertices()
  }

  /**
    * for this example, your nebula edge schema should have property names: descr, timp
    * if your withSrcAsProperty is true, then edge schema also should have property name: src
    * if your withDstAsProperty is true, then edge schema also should have property name: dst
    * if your withRankAsProperty is true, then edge schema also should have property name: degree
    */
  def writeEdge(spark: SparkSession): Unit = {
    LOG.info("start to write nebula edges")
    val df = spark.read.json("example/src/main/resources/edge")
    df.show()
    df.persist(StorageLevel.MEMORY_AND_DISK_SER)

    val config = getNebulaConnectionConfig()
    val nebulaWriteEdgeConfig: WriteNebulaEdgeConfig = WriteNebulaEdgeConfig
      .builder()
      .withSpace("test")
      .withEdge("friend")
      .withSrcIdField("src")
      .withDstIdField("dst")
      .withRankField("degree")
      .withSrcAsProperty(false)
      .withDstAsProperty(false)
      .withRankAsProperty(false)
      .withBatch(1000)
      .build()
    df.write.nebula(config, nebulaWriteEdgeConfig).writeEdges()
  }

  /**
    * We only update property that exists in DataFrame. For this example, update property `name`.
    */
  def updateVertex(spark: SparkSession): Unit = {
    LOG.info("start to write nebula vertices")
    val df = spark.read.json("example/src/main/resources/vertex").select("id", "age")
    df.show()

    val config = getNebulaConnectionConfig()
    val nebulaWriteVertexConfig: WriteNebulaVertexConfig = WriteNebulaVertexConfig
      .builder()
      .withSpace("test")
      .withTag("person")
      .withVidField("id")
      .withVidAsProp(false)
      .withBatch(1000)
      .withWriteMode(WriteMode.UPDATE)
      .build()
    df.write.nebula(config, nebulaWriteVertexConfig).writeVertices()
  }

  /**
    * we only update property that exists in DataFrame. For this example, we only update property `descr`.
    * if withRankField is not set when execute {@link writeEdge}, then don't set it too in this example.
    */
  def updateEdge(spark: SparkSession): Unit = {
    LOG.info("start to write nebula edges")
    val df = spark.read
      .json("example/src/main/resources/edge")
      .select("src", "dst", "degree", "descr")
    df.show()
    df.persist(StorageLevel.MEMORY_AND_DISK_SER)

    val config = getNebulaConnectionConfig()
    val nebulaWriteEdgeConfig: WriteNebulaEdgeConfig = WriteNebulaEdgeConfig
      .builder()
      .withSpace("test")
      .withEdge("friend")
      .withSrcIdField("src")
      .withDstIdField("dst")
      .withRankField("degree")
      .withSrcAsProperty(false)
      .withDstAsProperty(false)
      .withRankAsProperty(false)
      .withBatch(1000)
      .withWriteMode(WriteMode.UPDATE)
      .build()
    df.write.nebula(config, nebulaWriteEdgeConfig).writeEdges()
  }

  def deleteVertex(spark: SparkSession): Unit = {
    LOG.info("start to delete nebula vertices")
    val df = spark.read
      .json("example/src/main/resources/vertex")
      .select("id")
    df.show()
    df.persist(StorageLevel.MEMORY_AND_DISK_SER)

    val config = getNebulaConnectionConfig()
    val nebulaWriteVertexConfig: WriteNebulaVertexConfig = WriteNebulaVertexConfig
      .builder()
      .withSpace("test")
      .withVidField("id")
      .withBatch(1)
      .withUser("root")
      .withPasswd("nebula")
      .withWriteMode(WriteMode.DELETE)
      // config deleteEdge true, means delete related edges when delete vertex
      // refer https://docs.nebula-graph.com.cn/master/3.ngql-guide/12.vertex-statements/4.delete-vertex/#_1
      .withDeleteEdge(true)
      .build()
    df.write.nebula(config, nebulaWriteVertexConfig).writeVertices()
  }

  def deleteEdge(spark: SparkSession): Unit = {
    LOG.info("start to delete nebula edges")
    val df = spark.read
      .json("example/src/main/resources/edge")
      .select("src", "dst", "degree")
    df.show()
    df.persist(StorageLevel.MEMORY_AND_DISK_SER)

    val config = getNebulaConnectionConfig()
    val nebulaWriteEdgeConfig: WriteNebulaEdgeConfig = WriteNebulaEdgeConfig
      .builder()
      .withSpace("test")
      .withEdge("friend")
      .withSrcIdField("src")
      .withDstIdField("dst")
      .withRankField("degree")
      .withBatch(10)
      .withWriteMode(WriteMode.DELETE)
      .build()
    df.write.nebula(config, nebulaWriteEdgeConfig).writeEdges()
  }

}
