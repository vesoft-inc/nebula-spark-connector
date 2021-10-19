/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

package com.vesoft.nebula.connector.mock

import com.facebook.thrift.protocol.TCompactProtocol
import com.vesoft.nebula.connector.connector.NebulaDataFrameWriter
import com.vesoft.nebula.connector.{
  NebulaConnectionConfig,
  WriteMode,
  WriteNebulaEdgeConfig,
  WriteNebulaVertexConfig
}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object SparkMock {

  /**
    * write nebula vertex with insert mode
    */
  def writeVertex(): Unit = {
    val sparkConf = new SparkConf
    sparkConf
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .registerKryoClasses(Array[Class[_]](classOf[TCompactProtocol]))
    val spark = SparkSession
      .builder()
      .master("local")
      .config(sparkConf)
      .getOrCreate()

    val df = spark.read
      .option("header", true)
      .csv("src/test/resources/vertex.csv")

    val config =
      NebulaConnectionConfig
        .builder()
        .withMetaAddress("127.0.0.1:9559")
        .withGraphAddress("127.0.0.1:9669")
        .withConenctionRetry(2)
        .build()
    val nebulaWriteVertexConfig: WriteNebulaVertexConfig = WriteNebulaVertexConfig
      .builder()
      .withSpace("test_write_string")
      .withTag("person_connector")
      .withVidField("id")
      .withVidAsProp(false)
      .withBatch(5)
      .build()
    df.write.nebula(config, nebulaWriteVertexConfig).writeVertices()

    spark.stop()
  }

  /**
    * write nebula vertex with delete mode
    */
  def deleteVertex(): Unit = {
    val sparkConf = new SparkConf
    sparkConf
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .registerKryoClasses(Array[Class[_]](classOf[TCompactProtocol]))
    val spark = SparkSession
      .builder()
      .master("local")
      .config(sparkConf)
      .getOrCreate()

    val df = spark.read
      .option("header", true)
      .csv("src/test/resources/vertex.csv")

    val config =
      NebulaConnectionConfig
        .builder()
        .withMetaAddress("127.0.0.1:9559")
        .withGraphAddress("127.0.0.1:9669")
        .withConenctionRetry(2)
        .build()
    val nebulaWriteVertexConfig: WriteNebulaVertexConfig = WriteNebulaVertexConfig
      .builder()
      .withSpace("test_write_string")
      .withTag("person_connector")
      .withVidField("id")
      .withVidAsProp(false)
      .withWriteMode(WriteMode.DELETE)
      .withBatch(5)
      .build()
    df.write.nebula(config, nebulaWriteVertexConfig).writeVertices()

    spark.stop()
  }

  /**
    * write nebula edge with insert mode
    */
  def writeEdge(): Unit = {
    val sparkConf = new SparkConf
    sparkConf
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .registerKryoClasses(Array[Class[_]](classOf[TCompactProtocol]))
    val spark = SparkSession
      .builder()
      .master("local")
      .config(sparkConf)
      .getOrCreate()

    val df = spark.read
      .option("header", true)
      .csv("src/test/resources/edge.csv")

    val config =
      NebulaConnectionConfig
        .builder()
        .withMetaAddress("127.0.0.1:9559")
        .withGraphAddress("127.0.0.1:9669")
        .withConenctionRetry(2)
        .build()
    val nebulaWriteEdgeConfig: WriteNebulaEdgeConfig = WriteNebulaEdgeConfig
      .builder()
      .withSpace("test_write_string")
      .withEdge("friend_connector")
      .withSrcIdField("id1")
      .withDstIdField("id2")
      .withRankField("col3")
      .withRankAsProperty(true)
      .withBatch(5)
      .build()
    df.write.nebula(config, nebulaWriteEdgeConfig).writeEdges()

    spark.stop()
  }

  /**
    * write nebula edge with delete mode
    */
  def deleteEdge(): Unit = {
    val sparkConf = new SparkConf
    sparkConf
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .registerKryoClasses(Array[Class[_]](classOf[TCompactProtocol]))
    val spark = SparkSession
      .builder()
      .master("local")
      .config(sparkConf)
      .getOrCreate()

    val df = spark.read
      .option("header", true)
      .csv("src/test/resources/edge.csv")

    val config =
      NebulaConnectionConfig
        .builder()
        .withMetaAddress("127.0.0.1:9559")
        .withGraphAddress("127.0.0.1:9669")
        .withConenctionRetry(2)
        .build()
    val nebulaWriteEdgeConfig: WriteNebulaEdgeConfig = WriteNebulaEdgeConfig
      .builder()
      .withSpace("test_write_string")
      .withEdge("friend_connector")
      .withSrcIdField("id1")
      .withDstIdField("id2")
      .withRankField("col3")
      .withRankAsProperty(true)
      .withWriteMode(WriteMode.DELETE)
      .withBatch(5)
      .build()
    df.write.nebula(config, nebulaWriteEdgeConfig).writeEdges()

    spark.stop()
  }

}
