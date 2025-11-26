/*
 * Copyright (c) 2025 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

package com.vesoft.nebula.spark.common

import com.vesoft.nebula.spark.common.nebula.{GraphProvider}
import org.apache.spark.sql.catalyst.util.CaseInsensitiveMap
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

import java.util
import java.util.concurrent.TimeUnit
import scala.collection.JavaConversions.asJavaCollection
import scala.collection.JavaConverters.asScalaBufferConverter

class GraphProviderSuite extends AnyFunSuite with BeforeAndAfterAll {
  var graphProvider: GraphProvider = null
  val graphName                    = "spark_connector_nba"

  override def beforeAll(): Unit = {
    val address     = TestServerInfo.host
    val authOptions = new util.HashMap[String, Object]()
    authOptions.put("password", TestServerInfo.passwd)
    val map           = CaseInsensitiveMap(Map[String, String]("password" -> TestServerInfo.passwd,
                                                               NebulaOptions.GRAPH_ADDRESS -> address,
                                                               NebulaOptions.USER_NAME -> TestServerInfo.user,
                                                               NebulaOptions.PASSWD -> TestServerInfo.passwd,
                                                               NebulaOptions.TIMEOUT -> "3000",
                                                               NebulaOptions.GRAPH_NAME -> "nba",
                                                               NebulaOptions.SCHEMA -> "/default_schema",
                                                               NebulaOptions.ZONED_DATETIME_FORMAT -> "%Y-%m-%dT%H:%M:%S %z",
                                                               NebulaOptions.ZONED_TIME_FORMAT -> "%H:%M:%S %z",
                                                               NebulaOptions.LOCAL_DATETIME_FORMAT -> "%Y-%m-%dT%H:%M:%S",
                                                               NebulaOptions.LOCAL_TIME_FORMAT -> "%H:%M:%S",
                                                               NebulaOptions.OPERATE_TYPE -> "write",
                                                               NebulaOptions.TYPE -> "node",
                                                               NebulaOptions.LABEL -> "person"))
    val nebulaOptions = new NebulaOptions(map)
    graphProvider = new GraphProvider(nebulaOptions)

    val createSchema = "CREATE GRAPH TYPE IF NOT EXISTS spark_connector_nba_type AS {" +
      "NODE TYPE node_type_player (LABEL player {id INT PRIMARY KEY, name STRING, score FLOAT, gender bool, rate DOUBLE})," +
      "EDGE TYPE edge_type_follow (node_type_player)-[LABEL follow {followness INT, likeness FLOAT64}]->(node_type_player)}"
    val createGraph  = "CREATE GRAPH IF NOT EXISTS spark_connector_nba TYPED spark_connector_nba_type"
    val resp         = graphProvider.submit(createSchema)
    if (!resp.isSucceeded) {
      System.out.println("create graph type failed, " + resp.getErrorMessage)
      graphProvider.close()
      System.exit(1)
    }
    TimeUnit.SECONDS.sleep(5)
  }

  override def afterAll(): Unit = graphProvider.close()


  test("getNodeDesc") {
    val nodeDesc = graphProvider.getNodeDesc(graphName, "node_type_player")
    assert(nodeDesc.nodeTypeName.equals("node_type_player"))
    assert(nodeDesc.properties.size == 5)
    assert(nodeDesc.properties.keySet.contains("id"))
    assert(nodeDesc.properties.keySet.contains("name"))
    assert(nodeDesc.properties.keySet.contains("score"))
    assert(nodeDesc.properties.keySet.contains("gender"))
    assert(nodeDesc.properties.keySet.contains("rate"))
  }

  test("getEdgeDesc") {
    val edgeDesc = graphProvider.getEdgeDesc(graphName, "edge_type_follow")
    assert(edgeDesc.edgeTypeName.equals("edge_type_follow"))
    assert(edgeDesc.srcNodeTypeName.equals("node_type_player"))
    assert(edgeDesc.dstNodeTypeName.equals("node_type_player"))
    assert(edgeDesc.srcNodePkDataTypeMap("id").equals("INT64"))
    assert(edgeDesc.dstNodePkDataTypeMap("id").equals("INT64"))
    assert(edgeDesc.properties.size == 2)
    assert(edgeDesc.properties.keySet.contains("followness"))
    assert(edgeDesc.properties.keySet.contains("likeness"))
  }

  test("getAllParts") {
    val parts: List[Integer] = graphProvider.getAllParts.asScala.toList
    assert(parts.size() == 10)
    val expectParts = List(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
    assert(parts.containsAll(expectParts))
  }

}
