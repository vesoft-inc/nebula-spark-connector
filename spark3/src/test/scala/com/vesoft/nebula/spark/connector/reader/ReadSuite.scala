/*
 * Copyright (c) 2025 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */
package com.vesoft.nebula.spark.connector.reader

import com.vesoft.nebula.spark.common.{NebulaConnectionConfig, ReadNebulaConfig}
import com.vesoft.nebula.spark.connector.{NebulaDataFrameReader, TestServerInfo}
import com.vesoft.nebula.spark.connector.mock.NebulaGraphMock
import org.apache.spark.sql.SparkSession
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

class ReadSuite extends AnyFunSuite with BeforeAndAfterAll {
  var sparkSession: SparkSession = null

  val graphAddr = TestServerInfo.host
  val user      = TestServerInfo.user
  val passwd    = TestServerInfo.passwd

  override def beforeAll(): Unit = {
    val graphMock = new NebulaGraphMock
    graphMock.mockReadGraph()
    sparkSession = SparkSession.builder().master("local").getOrCreate()
  }

  override def afterAll(): Unit = {
    sparkSession.stop()
  }

  test("read node with no property") {
    val config     = NebulaConnectionConfig
      .builder()
      .withGraphAddress(graphAddr)
      .withUser(user)
      .withPasswd(passwd)
      .build()
    val readConfig = ReadNebulaConfig
      .builder()
      .withSchema("/default_schema")
      .withGraphName("spark3_read")
      .withTypeName("node_player")
      .withReturnCols(List())
      .withBatchSize(1)
      .withPartitionNum(10)
      .build()
    val nodeData   = sparkSession.read.nebula(config, readConfig).loadNode()
    nodeData.printSchema()
    nodeData.show(truncate = false)
    assert(nodeData.count == 13)
    assert(nodeData.schema.fields.length == 1)
  }

  test("read node with all property") {
    val config     = NebulaConnectionConfig
      .builder()
      .withGraphAddress(graphAddr)
      .withUser(user)
      .withPasswd(passwd)
      .build()
    val readConfig = ReadNebulaConfig
      .builder()
      .withGraphName("spark3_read")
      .withTypeName("node_player")
      .withReturnCols(null)
      .withBatchSize(1)
      .withPartitionNum(10)
      .build()
    val nodeData   = sparkSession.read.nebula(config, readConfig).loadNode()
    nodeData.printSchema()
    nodeData.show(truncate = false)
    assert(nodeData.count == 13)
    assert(nodeData.schema.fields.length == 9)
  }

  test("read node with specific properties") {
    val config     = NebulaConnectionConfig
      .builder()
      .withGraphAddress(graphAddr)
      .withUser(user)
      .withPasswd(passwd)
      .build()
    val readConfig = ReadNebulaConfig
      .builder()
      .withGraphName("spark3_read")
      .withTypeName("node_player")
      .withReturnCols(List("col1", "col2", "col3", "col4"))
      .withBatchSize(1)
      .withPartitionNum(10)
      .build()
    val nodeData   = sparkSession.read.nebula(config, readConfig).loadNode()
    nodeData.printSchema()
    nodeData.show(truncate = false)
    assert(nodeData.count == 13)
    assert(nodeData.schema.fields.length == 4)
  }


  test("read edge with no property") {
    val config     = NebulaConnectionConfig
      .builder()
      .withGraphAddress(graphAddr)
      .withUser(user)
      .withPasswd(passwd)
      .build()
    val readConfig = ReadNebulaConfig
      .builder()
      .withGraphName("spark3_read")
      .withTypeName("edge_follow")
      .withReturnCols(List())
      .withBatchSize(1)
      .withPartitionNum(10)
      .build()
    val edgeData   = sparkSession.read.nebula(config, readConfig).loadEdge()
    edgeData.printSchema()
    edgeData.show(truncate = false)
    assert(edgeData.count == 10)
    assert(edgeData.schema.fields.length == 2)
  }

  test("read edge with all property") {
    val config     = NebulaConnectionConfig
      .builder()
      .withGraphAddress(graphAddr)
      .withUser(user)
      .withPasswd(passwd)
      .build()
    val readConfig = ReadNebulaConfig
      .builder()
      .withGraphName("spark3_read")
      .withTypeName("edge_follow")
      .withReturnCols(null)
      .withBatchSize(1)
      .withPartitionNum(10)
      .build()
    val edgeData   = sparkSession.read.nebula(config, readConfig).loadEdge()
    edgeData.printSchema()
    edgeData.show(truncate = false)
    assert(edgeData.count == 10)
    assert(edgeData.schema.fields.length == 6)
  }

  test("read edge with specific properties") {
    val config     = NebulaConnectionConfig
      .builder()
      .withGraphAddress(graphAddr)
      .withUser(user)
      .withPasswd(passwd)
      .build()
    val readConfig = ReadNebulaConfig
      .builder()
      .withGraphName("spark3_read")
      .withTypeName("edge_follow")
      .withReturnCols(List("ecol1", "ecol2"))
      .withBatchSize(1)
      .withPartitionNum(10)
      .build()
    val edgeData   = sparkSession.read.nebula(config, readConfig).loadEdge()
    edgeData.printSchema()
    edgeData.show(truncate = false)
    assert(edgeData.count == 10)
    assert(edgeData.schema.fields.length == 4)
  }
}
