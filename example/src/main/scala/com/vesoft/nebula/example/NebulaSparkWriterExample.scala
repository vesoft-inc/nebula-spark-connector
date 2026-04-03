/*
 * Copyright (c) 2025 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

package com.vesoft.nebula.example

import com.sun.org.slf4j.internal.LoggerFactory
import com.vesoft.nebula.spark.connector.NebulaDataFrameWriter
import com.vesoft.nebula.driver.graph.net.NebulaClient
import com.vesoft.nebula.spark.common.{NebulaConnectionConfig, WriteMode, WriteNebulaEdgeConfig, WriteNebulaNodeConfig}
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel

object NebulaSparkWriterExample {
  private val LOG = LoggerFactory.getLogger(this.getClass)

  def main(args: Array[String]): Unit = {
    prepareSchema()
    val spark = SparkSession
      .builder()
      .master("local")
      .getOrCreate()

    writeNode(spark)
    writeEdge(spark)
//    deleteNode(spark)
//    deleteEdge(spark)
    updateEdge(spark)

    spark.close()
  }

  private def prepareSchema(): Unit = {
    var client: NebulaClient = null
    try {
      client = NebulaClient.builder("192.168.8.6:3820", "root", "Nebula123").build()
      val createSchema = "CREATE GRAPH TYPE IF NOT EXISTS graph_type_nba AS {" +
        "NODE TYPE node_type_player (LABEL player {id INT, name STRING, score FLOAT, gender bool, rate DOUBLE, primary key(id, name)})," +
        "EDGE TYPE edge_type_follow(node_type_player)-[LABEL follow {followness INT, likeness FLOAT64}]->(node_type_player)}"
      var resp         = client.execute(createSchema)
      if (!resp.isSucceeded) {
        println("create graph type failed, " + resp.getErrorMessage)
        System.exit(1)
      }
      else {
        println("create graph type succeed!")
      }

      val createGraph = "CREATE GRAPH IF NOT EXISTS nba TYPED graph_type_nba"
      resp = client.execute(createGraph)
      if (!resp.isSucceeded) {
        println("create graph failed, " + resp.getErrorMessage)
        System.exit(1)
      } else {
        println("create graph succeed!")
      }
    } finally {
      if (client != null) {
        client.close()
      }
    }
  }

  private def getNebulaConnectionConfig: NebulaConnectionConfig = {
    val authOptions = Map("password" -> "Nebula123")
    NebulaConnectionConfig
      .builder()
      .withGraphAddress("192.168.8.6:3820")
      .withUser("root")
      .withPasswd("Nebula123")
      .withAuthOptions(authOptions)
      .withPingTimeoutSec(5)
      .build()
  }

  /**
   * for this example, your nebula tag schema should have property names: id, name, age, born
   */
  private def writeNode(spark: SparkSession): Unit = {
    val df = spark.read.json("example/src/main/resources/vertex")
    df.show()

    val nebulaWriteNodeConfig: WriteNebulaNodeConfig = WriteNebulaNodeConfig
      .builder()
      .withGraphName("nba")
      //.withSchema("/default_schema")
      //.withZonedDatetimeFormat("%Y-%m-%dT%H:%M:%S %z")
      //.withLocalDatetimeFormat("%Y-%m-%dT%H:%M:%S")
      //.withZonedTimeFormat("%H:%M:%S %z")
      //.withLocalTimeFormat("%H:%M:%S")
      .withNodeType("node_type_player")
      .withWriteMode(WriteMode.INSERTIGNORE)
      .withBatchSize(10)
      .build()
    df.write.nebula(getNebulaConnectionConfig, nebulaWriteNodeConfig).writeNodes()
  }

  /**
   * for this example, your nebula edge schema should have property names: followness, linkeness
   * if your withSrcAsProperty is true, then edge schema also should have property name: src
   * if your withDstAsProperty is true, then edge schema also should have property name: dst
   */
  private def writeEdge(spark: SparkSession): Unit = {
    val df = spark.read.json("example/src/main/resources/edge")
    df.show()
    df.persist(StorageLevel.MEMORY_AND_DISK_SER)

    val nebulaWriteEdgeConfig: WriteNebulaEdgeConfig = WriteNebulaEdgeConfig
      .builder()
      .withGraphName("nba")
      //.withSchema("/default_schema")
      //.withZonedDatetimeFormat("%Y-%m-%dT%H:%M:%S %z")
      //.withLocalDatetimeFormat("%Y-%m-%dT%H:%M:%S")
      //.withZonedTimeFormat("%H:%M:%S %z")
      //.withLocalTimeFormat("%H:%M:%S")
      .withEdge("edge_type_follow")
      .withSrcPkFields(List("src","name1"))
      .withDstPkFields(List("dst","name2"))
      .withSrcPksAsProperty(false)
      .withDstPksAsProperty(false)
      .withWriteMode(WriteMode.INSERTIGNORE)
      .withBatchSize(10)
      .build()
    df.write.nebula(getNebulaConnectionConfig, nebulaWriteEdgeConfig).writeEdges()
  }


  private def deleteNode(spark: SparkSession): Unit = {
    val df = spark.read.json("example/src/main/resources/vertex")
    df.show()

    val nebulaWriteNodeConfig: WriteNebulaNodeConfig = WriteNebulaNodeConfig
      .builder()
      .withGraphName("nba")
      .withNodeType("node_type_player")
      .withWriteMode(WriteMode.DELETE)
      .withBatchSize(10)
      .build()
    df.write.nebula(getNebulaConnectionConfig, nebulaWriteNodeConfig).writeVertices()
  }


  private def deleteEdge(spark: SparkSession): Unit = {
    val df = spark.read.json("example/src/main/resources/edge")
    df.show()
    df.persist(StorageLevel.MEMORY_AND_DISK_SER)

    val nebulaWriteEdgeConfig: WriteNebulaEdgeConfig = WriteNebulaEdgeConfig
      .builder()
      .withGraphName("nba")
      .withEdge("edge_type_follow")
      .withSrcPkFields(List("src","name1"))
      .withDstPkFields(List("dst", "name2"))
      .withWriteMode(WriteMode.DELETE)
      .withBatchSize(2)
      .build()
    df.write.nebula(getNebulaConnectionConfig, nebulaWriteEdgeConfig).writeEdges()
  }

  private def updateEdge(spark: SparkSession): Unit = {
    val df = spark.read.json("example/src/main/resources/edge")
    df.show()
    df.persist(StorageLevel.MEMORY_AND_DISK_SER)

    val nebulaWriteEdgeConfig: WriteNebulaEdgeConfig = WriteNebulaEdgeConfig
      .builder()
      .withGraphName("nba")
      .withEdge("edge_type_follow")
      .withSrcPkFields(List("src","name1"))
      .withDstPkFields(List("dst", "name2"))
      .withWriteMode(WriteMode.UPDATE)
      .withBatchSize(2)
      .build()
    df.write.nebula(getNebulaConnectionConfig, nebulaWriteEdgeConfig).writeEdges()
  }
}
