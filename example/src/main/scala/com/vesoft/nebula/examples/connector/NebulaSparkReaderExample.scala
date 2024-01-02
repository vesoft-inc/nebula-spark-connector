/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

package com.vesoft.nebula.examples.connector

import com.facebook.thrift.protocol.TCompactProtocol
import com.vesoft.nebula.connector.connector.NebulaDataFrameReader
import com.vesoft.nebula.connector.ssl.SSLSignType
import com.vesoft.nebula.connector.{NebulaConnectionConfig, ReadNebulaConfig}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory

object NebulaSparkReaderExample {

  private val LOG = LoggerFactory.getLogger(this.getClass)

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf
    sparkConf
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .registerKryoClasses(Array[Class[_]](classOf[TCompactProtocol]))
    val spark = SparkSession
      .builder()
      .master("local")
      .config(sparkConf)
      .getOrCreate()

    readVertex(spark)
    readEdges(spark)
    readVertexGraph(spark)
    readEdgeGraph(spark)
    //readEdgeWithNgql(spark)

    spark.close()
    sys.exit()
  }

  def readVertex(spark: SparkSession): Unit = {
    val config =
      NebulaConnectionConfig
        .builder()
        .withMetaAddress("127.0.0.1:9559")
        .withGraphAddress("127.0.0.1:9669")
        .withConnectionRetry(2)
        .withVersion("3.0.0")
        .build()
    val nebulaReadVertexConfig: ReadNebulaConfig = ReadNebulaConfig
      .builder()
      .withSpace("test")
      .withLabel("person")
      .withNoColumn(false)
      .withReturnCols(List())
      .withLimit(10)
      .withPartitionNum(10)
      .withUser("root")
      .withPasswd("nebula")
      .build()
    val vertex = spark.read.nebula(config, nebulaReadVertexConfig).loadVerticesToDF()
    vertex.printSchema()
    vertex.show(20)
    println("vertex count: " + vertex.count())
  }

  def readEdges(spark: SparkSession): Unit = {
    val config =
      NebulaConnectionConfig
        .builder()
        .withMetaAddress("127.0.0.1:9559")
        .withGraphAddress("127.0.0.1:9669")
        .withTimeout(6000)
        .withConnectionRetry(2)
        .withVersion("3.0.0")
        .build()
    val nebulaReadEdgeConfig: ReadNebulaConfig = ReadNebulaConfig
      .builder()
      .withSpace("test")
      .withLabel("knows")
      .withNoColumn(false)
      .withReturnCols(List())
      .withLimit(10)
      .withPartitionNum(10)
      .withUser("root")
      .withPasswd("nebula")
      .build()
    val edge = spark.read.nebula(config, nebulaReadEdgeConfig).loadEdgesToDF()
    edge.printSchema()
    edge.show(20)
    println("edge count: " + edge.count())
  }

  def readVertexGraph(spark: SparkSession): Unit = {
    LOG.info("start to read graphx vertex")
    val config =
      NebulaConnectionConfig
        .builder()
        .withMetaAddress("127.0.0.1:9559")
        .withGraphAddress("127.0.0.1:9669")
        .withTimeout(6000)
        .withConnectionRetry(2)
        .withVersion("3.0.0")
        .build()
    val nebulaReadVertexConfig: ReadNebulaConfig = ReadNebulaConfig
      .builder()
      .withSpace("test")
      .withLabel("person")
      .withNoColumn(false)
      .withReturnCols(List("birthday"))
      .withLimit(10)
      .withPartitionNum(10)
      .withUser("root")
      .withPasswd("nebula")
      .build()

    val vertexRDD = spark.read.nebula(config, nebulaReadVertexConfig).loadVerticesToGraphx()
    LOG.info("vertex rdd first record: " + vertexRDD.first())
    LOG.info("vertex rdd count: {}", vertexRDD.count())
  }

  def readEdgeGraph(spark: SparkSession): Unit = {
    LOG.info("start to read graphx edge")
    val config =
      NebulaConnectionConfig
        .builder()
        .withMetaAddress("127.0.0.1:9559")
        .withGraphAddress("127.0.0.1:9669")
        .withTimeout(6000)
        .withConnectionRetry(2)
        .withVersion("3.0.0")
        .build()
    val nebulaReadEdgeConfig: ReadNebulaConfig = ReadNebulaConfig
      .builder()
      .withSpace("test")
      .withLabel("knows")
      .withNoColumn(false)
      .withReturnCols(List("timep"))
      .withLimit(10)
      .withPartitionNum(10)
      .withUser("root")
      .withPasswd("nebula")
      .build()
    val edgeRDD = spark.read.nebula(config, nebulaReadEdgeConfig).loadEdgesToGraphx()
    LOG.info("edge rdd first record:" + edgeRDD.first())
    LOG.info("edge rdd count: {}", edgeRDD.count())
  }

  /**
    * read Nebula vertex with SSL
    */
  def readVertexWithSSL(spark: SparkSession): Unit = {
    LOG.info("start to read nebula vertices with ssl")
    val config =
      NebulaConnectionConfig
        .builder()
        .withMetaAddress("127.0.0.1:9559")
        .withGraphAddress("127.0.0.1:9669")
        .withEnableMetaSSL(true)
        .withEnableStorageSSL(true)
        .withSSLSignType(SSLSignType.CA)
        .withCaSSLSignParam("example/src/main/resources/ssl/casigned.pem",
                            "example/src/main/resources/ssl/casigned.crt",
                            "example/src/main/resources/ssl/casigned.key")
        .withConnectionRetry(2)
        .withVersion("3.0.0")
        .build()
    val nebulaReadVertexConfig: ReadNebulaConfig = ReadNebulaConfig
      .builder()
      .withSpace("test")
      .withLabel("person")
      .withNoColumn(false)
      .withReturnCols(List("birthday"))
      .withLimit(10)
      .withPartitionNum(10)
      .withUser("root")
      .withPasswd("nebula")
      .build()
    val vertex = spark.read.nebula(config, nebulaReadVertexConfig).loadVerticesToDF()
    vertex.printSchema()
    vertex.show(20)
    println("vertex count: " + vertex.count())
  }

  /*
  def readEdgeWithNgql(spark: SparkSession): Unit = {
    LOG.info("start to read nebula edge with ngql")
    val config =
      NebulaConnectionConfig
        .builder()
        .withMetaAddress("127.0.0.1:9559")
        .withGraphAddress("127.0.0.1:9669")
        .withConnectionRetry(2)
        .build()
    val nebulaReadConfig: ReadNebulaConfig = ReadNebulaConfig
      .builder()
      .withSpace("test")
      .withLabel("friend")
      // please make sure you have config the NoColumn true or returnCols with at least one column.
      //.withNoColumn(true)
      .withReturnCols(List("degree"))
      // please make sure your ngql statement result is edge, connector does not check the statement.
      // other examples of supported nGQL:
      // - GET SUBGRAPH WITH PROP 3 STEPS FROM 2 YIELD EDGES AS relationships;
      // - FIND ALL PATH WITH PROP FROM 2 TO 4 OVER friend YIELD path AS p;
      .withNgql("match (v)-[e:friend]->(v2) return e")
      .build()
    val edge = spark.read.nebula(config, nebulaReadConfig).loadEdgesToDfByNgql()
    edge.printSchema()
    edge.show(20)
    println("edge count: " + edge.count())
  }
   */
}
