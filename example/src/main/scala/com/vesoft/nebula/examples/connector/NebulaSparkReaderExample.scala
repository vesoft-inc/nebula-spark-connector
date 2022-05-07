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

    spark.close()
    sys.exit()
  }

  def readVertex(spark: SparkSession): Unit = {
    LOG.info("start to read nebula vertices")
    val config =
      NebulaConnectionConfig
        .builder()
        .withMetaAddress("127.0.0.1:9559")
        .withConenctionRetry(2)
        .build()
    val nebulaReadVertexConfig: ReadNebulaConfig = ReadNebulaConfig
      .builder()
      .withSpace("test")
      .withLabel("person")
      .withNoColumn(false)
      .withReturnCols(List("birthday"))
      .withLimit(10)
      .withPartitionNum(10)
      .build()
    val vertex = spark.read.nebula(config, nebulaReadVertexConfig).loadVerticesToDF()
    vertex.printSchema()
    vertex.show(20)
    println("vertex count: " + vertex.count())
  }

  def readEdges(spark: SparkSession): Unit = {
    LOG.info("start to read nebula edges")

    val config =
      NebulaConnectionConfig
        .builder()
        .withMetaAddress("127.0.0.1:9559")
        .withTimeout(6000)
        .withConenctionRetry(2)
        .build()
    val nebulaReadEdgeConfig: ReadNebulaConfig = ReadNebulaConfig
      .builder()
      .withSpace("test")
      .withLabel("knows")
      .withNoColumn(false)
      .withReturnCols(List("degree"))
      .withLimit(10)
      .withPartitionNum(10)
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
        .withTimeout(6000)
        .withConenctionRetry(2)
        .build()
    val nebulaReadVertexConfig: ReadNebulaConfig = ReadNebulaConfig
      .builder()
      .withSpace("test")
      .withLabel("person")
      .withNoColumn(false)
      .withReturnCols(List("birthday"))
      .withLimit(10)
      .withPartitionNum(10)
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
        .withTimeout(6000)
        .withConenctionRetry(2)
        .build()
    val nebulaReadEdgeConfig: ReadNebulaConfig = ReadNebulaConfig
      .builder()
      .withSpace("test")
      .withLabel("knows")
      .withNoColumn(false)
      .withReturnCols(List("timep"))
      .withLimit(10)
      .withPartitionNum(10)
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
        .withEnableMetaSSL(true)
        .withEnableStorageSSL(true)
        .withSSLSignType(SSLSignType.CA)
        .withCaSSLSignParam("example/src/main/resources/ssl/casigned.pem",
                            "example/src/main/resources/ssl/casigned.crt",
                            "example/src/main/resources/ssl/casigned.key")
        .withConenctionRetry(2)
        .build()
    val nebulaReadVertexConfig: ReadNebulaConfig = ReadNebulaConfig
      .builder()
      .withSpace("test")
      .withLabel("person")
      .withNoColumn(false)
      .withReturnCols(List("birthday"))
      .withLimit(10)
      .withPartitionNum(10)
      .build()
    val vertex = spark.read.nebula(config, nebulaReadVertexConfig).loadVerticesToDF()
    vertex.printSchema()
    vertex.show(20)
    println("vertex count: " + vertex.count())
  }
}
  /**
    * read Nebula vertex with storaged addresses manually specified.
    */

  def readVertexGraph(spark: SparkSession): Unit = {
    LOG.info("start to read graphx vertex")
    val config =
      NebulaConnectionConfig
        .builder()
        .withMetaAddress("127.0.0.1:9559")
        .withStorageAddress("127.0.0.1:9779")
        .withTimeout(6000)
        .withConenctionRetry(2)
        .build()
    val nebulaReadVertexConfig: ReadNebulaConfig = ReadNebulaConfig
      .builder()
      .withSpace("test")
      .withLabel("person")
      .withNoColumn(false)
      .withReturnCols(List("birthday"))
      .withLimit(10)
      .withPartitionNum(10)
      .build()

    val vertexRDD = spark.read.nebula(config, nebulaReadVertexConfig).loadVerticesToGraphx()
    LOG.info("vertex rdd first record: " + vertexRDD.first())
    LOG.info("vertex rdd count: {}", vertexRDD.count())
  }