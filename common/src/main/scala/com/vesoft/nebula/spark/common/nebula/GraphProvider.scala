/*
 * Copyright (c) 2025 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

package com.vesoft.nebula.spark.common.nebula

import com.vesoft.nebula.driver.graph.data.{ResultSet, ValueWrapper}
import com.vesoft.nebula.driver.graph.net.{NebulaClient, NebulaPool}
import com.vesoft.nebula.driver.graph.scan.{ScanEdgeResultIterator, ScanNodeResultIterator}
import com.vesoft.nebula.spark.common.{NebulaOptions, NebulaUtils}
import org.slf4j.LoggerFactory

import java.util
import java.util.List
import scala.collection.JavaConverters.asScalaBufferConverter
import scala.collection.mutable
import scala.collection.mutable.{ArrayBuffer, ListBuffer}

/**
 * GraphProvider for Nebula Graph Service
 */
class GraphProvider(nebulaOptions: NebulaOptions) extends AutoCloseable with Serializable {
  @transient private[this] lazy val LOG = LoggerFactory.getLogger(this.getClass)

  private val addr: Seq[String] = nebulaOptions.graphAddress.split(",").toList
  private val randomAddr        = scala.util.Random.shuffle(addr)

  @transient private val poolBuilder: NebulaPool.Builder = NebulaPool
    .builder(randomAddr.mkString(","), nebulaOptions.user)
    .withAuthOptions(nebulaOptions.authOptions)
    .withConnectTimeoutMills(nebulaOptions.timeout * 1000)
    .withRequestTimeoutMills(nebulaOptions.timeout * 1000)
    .withEnableTls(nebulaOptions.enableTls)
    .withDisableVerifyServerCert(nebulaOptions.disableVerifyServerCert)
    .withTlsCa(nebulaOptions.tlsCa)
    .withTlsCert(nebulaOptions.tlsCert, nebulaOptions.tlsKey)
    .withBlockWhenExhausted(true)
    .withMaxWaitMills(10 * 60 * 1000)
    .withServerPingTimeoutMills(nebulaOptions.pingTimeout * 1000)

  if (nebulaOptions.schema != null && nebulaOptions.schema.nonEmpty) {
    poolBuilder.withSchema(nebulaOptions.schema)
  }
  if (nebulaOptions.dateFormat != null && nebulaOptions.dateFormat.nonEmpty) {
    poolBuilder.withDateFormat(nebulaOptions.dateFormat)
  }
  if (nebulaOptions.zonedDatetimeFormat != null && nebulaOptions.zonedDatetimeFormat.nonEmpty) {
    poolBuilder.withZonedDatetimeFormat(nebulaOptions.zonedDatetimeFormat)
  }

  if (nebulaOptions.localDatetimeFormat != null && nebulaOptions.localDatetimeFormat.nonEmpty) {
    poolBuilder.withLocalDatetimeFormat(nebulaOptions.localDatetimeFormat)
  }
  if (nebulaOptions.zonedTimeFormat != null && nebulaOptions.zonedTimeFormat.nonEmpty) {
    poolBuilder.withZonedTimeFormat(nebulaOptions.zonedTimeFormat)
  }

  if (nebulaOptions.localTimeFormat != null && nebulaOptions.localTimeFormat.nonEmpty) {
    poolBuilder.withLocalTimeFormat(nebulaOptions.localTimeFormat)
  }

  @transient val pool = poolBuilder.build()

  /**
   * close Nebula client
   */
  override def close(): Unit = {
    pool.close()
  }

  /**
   * execute the statement
   *
   * @param statement insert node/edge statement
   * @return execute result
   */
  def submit(statement: String): ResultSet = {
    val client         = pool.getClient
    var res: ResultSet = null
    try {
      res = client.execute(statement)
    } finally {
      pool.returnClient(client)
    }
    res
  }

  /**
   * scan node type
   *
   * @param graphName graph name
   * @param nodeType  node type name
   * @param part      NebulaGraph partition id
   * @param batchSize batchSize for each scan request
   * @return {@link ScanNodeResultIterator}
   */
  def scanNode(schema: String, graphName: String, nodeType: String, part: Int, batchSize: Int): ScanNodeResultIterator = {
    val client                      = pool.getClient
    var res: ScanNodeResultIterator = null
    try {
      res = client.scanNode(schema, graphName, nodeType, null, part, batchSize)
    } finally {
      pool.returnClient(client)
    }
    res
  }


  def scanNode(schema: String, graphName: String, nodeType: String, returnCols: util.List[String], part: Int, batchSize: Int): ScanNodeResultIterator = {
    val client                      = pool.getClient
    var res: ScanNodeResultIterator = null
    try {
      res = client.scanNode(schema, graphName, nodeType, returnCols, part, batchSize)
    } finally {
      pool.returnClient(client)
    }
    res
  }

  /**
   * scan edge type
   *
   * @param graphName graph name
   * @param edgeType  edge type name
   * @param part      NebulaGraph partition id
   * @param batchSize batchSize for each scan request
   * @return {@link ScanEdgeResultIterator}
   */
  def scanEdge(schema: String, graphName: String, edgeType: String, part: Int, batchSize: Int): ScanEdgeResultIterator = {
    val client                      = pool.getClient
    var res: ScanEdgeResultIterator = null
    try {
      res = client.scanEdge(schema, graphName, edgeType, null, part, batchSize)
    } finally {
      pool.returnClient(client)
    }
    res
  }


  def scanEdge(schema: String, graphName: String, edgeType: String, returnCols: util.List[String], part: Int, batchSize: Int): ScanEdgeResultIterator = {
    val client                      = pool.getClient
    var res: ScanEdgeResultIterator = null
    try {
      res = client.scanEdge(schema, graphName, edgeType, returnCols, part, batchSize)
    } finally {
      pool.returnClient(client)
    }
    res

  }

  /**
   * get all part list for NebulaGraph
   */
  def getAllParts: List[Integer] = {
    val showPartitions: String    = "CALL show_partitions() RETURN *"
    var resultSet     : ResultSet = null
    try resultSet = submit(showPartitions)
    catch {
      case e: Exception =>
        LOG.error("get all partitions error", e)
        throw new RuntimeException("get all partitions error", e)
    }
    if (!resultSet.isSucceeded || resultSet.isEmpty) {
      LOG.error("get all partitions failed for {}", resultSet.getErrorMessage)
      throw new RuntimeException("get all partitions failed for " + resultSet.getErrorMessage)
    }
    val partitions: util.List[Integer] = new util.ArrayList[Integer]
    while (resultSet.hasNext) {
      partitions.add(resultSet.next().get("partition_id").asInt())
    }
    partitions.remove(0)
    partitions
  }


  /**
   * get node's schema info
   *
   * @param graphName
   * @param nodeType
   * @return {@link NodeDesc}
   */
  def getNodeDesc(graphName: String, nodeType: String): NodeDesc = {
    val schema   : mutable.HashMap[String, String] = new mutable.HashMap[String, String]()
    val propNames: ArrayBuffer[String]             = new ArrayBuffer[String]()
    val graphType                                  = getGraphType(graphName)

    val escapedNodeType = NebulaUtils.escapeUtil(nodeType)
    val descNodeType    = s"DESCRIBE NODE TYPE `$escapedNodeType` OF `$graphType`"
    val result          = submit(descNodeType)
    if (!result.isSucceeded || result.isEmpty) {
      LOG.error(s"get schema of $nodeType failed for ${result.getErrorMessage}")
      throw new IllegalArgumentException(s"node type $escapedNodeType does not exist in $graphName.")
    }

    val pkNames = new ListBuffer[String]

    while (result.hasNext) {
      val record = result.next();
      propNames.append(record.get("property_name").asString())
      schema += (record.get("property_name").asString() -> record.get("data_type").asString())
      if (!record.get("primary_key").isNull && record.get("primary_key").asString().equals("Y")) {
        pkNames.append(record.get("property_name").asString())
      }
    }

    if (pkNames.isEmpty) {
      LOG.error(s"node type $nodeType has no primary key.")
      throw new RuntimeException(s"node type $nodeType has no primary key")
    }
    NodeDesc(nodeType, pkNames.toList, propNames, schema.toMap)
  }

  /**
   * get edge description info
   *
   * @param graphName
   * @param edgeType
   * @return {@link EdgeDesc}
   */
  def getEdgeDesc(graphName: String, edgeType: String): EdgeDesc = {
    val schema   : mutable.HashMap[String, String] = new mutable.HashMap[String, String]()
    val propNames: ArrayBuffer[String]             = new ArrayBuffer[String]()
    val graphType                                  = getGraphType(graphName)

    val escapedEdgeType     = NebulaUtils.escapeUtil(edgeType)
    val descEdgeTypePattern =
      s"call describe_graph_type('$graphType') filter type_name='$escapedEdgeType' return type_pattern,`primary_key/multiedge_key`"

    var result = submit(descEdgeTypePattern)
    if (!result.isSucceeded || result.isEmpty) {
      LOG.error(s"get edge type pattern of $edgeType failed for ${result.getErrorMessage}")
      throw new IllegalArgumentException(s"edge type $edgeType does not exist in $graphName.")
    }
    val record                  = result.next();
    val edgeTypePattern: String = record.get("type_pattern").asString()
    val edgeMultiKeysValue      = record.get("primary_key/multiedge_key")
    val multiEdgeKeyNames       = if (edgeMultiKeysValue.isList) {
      val names = new ListBuffer[String]
      edgeMultiKeysValue.asList().asScala.foreach(col => names.append(col.asString()))
      names.toList
    } else {
      scala.collection.immutable.List.empty[String]
    }

    result = submit(s"call describe_edge_type('$graphType', '$escapedEdgeType') return *")
    if (!result.isSucceeded) {
      LOG.error(s"desc edge type $edgeType failed for ${result.getErrorMessage}")
      throw new IllegalArgumentException(s"desc edge type $edgeType failed: ${result.getErrorMessage}")
    }
    while (result.hasNext) {
      val record = result.next()
      propNames.append(record.get("property_name").asString())
      schema += (record.get("property_name").asString() -> record.get("data_type").asString())
    }

    var srcNodeType: String       = null
    var dstNodeType: String       = null
    var isDirected : Boolean      = true
    // regularly match two types of edge:()-[]->() or ()~[]~() to get the srcNodeType and dstNodeType.
    val edgeDirectionPattern      = """\((.*?)\)-\[.*?\]->\((.*?)\)"""
    val edgeUnDirectionPattern    = """\((.*?)\)~\[.*?\]~\((.*?)\)"""
    val regexWithEdgeDirection    = edgeDirectionPattern.r
    val regexWithoutEdgeDirection = edgeUnDirectionPattern.r
    if (edgeTypePattern.matches(edgeDirectionPattern)) {
      isDirected = true
      edgeTypePattern match {
        case regexWithEdgeDirection(start, end) =>
          srcNodeType = start
          dstNodeType = end
      }
    } else if (edgeTypePattern.matches(edgeUnDirectionPattern)) {
      isDirected = false
      edgeTypePattern match {
        case regexWithoutEdgeDirection(start, end) =>
          srcNodeType = start
          dstNodeType = end
      }
    } else {
      throw new RuntimeException("can not parse the edge type pattern.")
    }

    val srcNodeDesc       = getNodeDesc(graphName, srcNodeType)
    val dstNodeDesc       = getNodeDesc(graphName, dstNodeType)
    val srcNodePkDataType = srcNodeDesc.properties.filterKeys(srcNodeDesc.nodePkNames.contains)
    val dstNodeIdDataType = dstNodeDesc.properties.filterKeys(dstNodeDesc.nodePkNames.contains)

    EdgeDesc(edgeType,
             isDirected,
             srcNodeType,
             srcNodeDesc.nodePkNames,
             srcNodePkDataType,
             dstNodeType,
             dstNodeDesc.nodePkNames,
             dstNodeIdDataType,
             multiEdgeKeyNames,
             propNames,
             schema.toMap)
  }

  private def getGraphType(graphName: String): String = {
    val escapedGraphName = NebulaUtils.escapeUtil(graphName)
    val resultSet        = submit(s"DESCRIBE GRAPH `$escapedGraphName`")
    val graphType        = if (resultSet.isSucceeded && !resultSet.isEmpty) {
      resultSet.next().values().get(1).asString
    } else {
      throw new IllegalArgumentException(s"graphName $graphName does not exist.")
    }
    graphType
  }
}

