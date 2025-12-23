/*
 * Copyright (c) 2025 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */
package com.vesoft.nebula.spark.common.writer

import com.vesoft.nebula.driver.graph.ErrorCode
import com.vesoft.nebula.spark.common.{NebulaNode, NebulaNodes, NebulaOptions, WriteMode}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types.StructType

import scala.collection.mutable.ListBuffer

class NodeWriter(nebulaOptions: NebulaOptions, schema: StructType) extends NebulaWriter(nebulaOptions) {

  private val nodeDesc = graphProvider.getNodeDesc(nebulaOptions.graphName, nebulaOptions.label)

  val fieldTypeMap: Map[String, String] = nodeDesc.properties
  private val pkNames: List[String] = nodeDesc.nodePkNames

  /** buffer to save batch vertices */
  protected val nodes: ListBuffer[NebulaNode] = new ListBuffer()

  def writeRow(row: InternalRow): Unit = {
    // check the node primary key value's validation, the pkName must exist in row,
    // it's already checked in NebulaDataSourceNodeWriter.createWriterFactory
    val pkIndicesInSparkRow: Map[String, Int] = schema.fields.toList.map(field => field.name).zip(schema.fields.indices).toMap
    pkIndicesInSparkRow.foreach((pkIndex) => {
      if (row.isNullAt(pkIndex._2) && pkNames.contains(pkIndex._1)) {
        LOG.error(s">>>> record has null value at index ${pkIndex._2} for primary key ${pkIndex._1}, ignore it. record:$row")
        return
      }
    })

    val values     =
      if (nebulaOptions.writeMode == WriteMode.DELETE) {
        NebulaExecutor.assignNodePkValues(schema, row, fieldTypeMap, pkNames)
      } else {
        NebulaExecutor.assignNodePropValues(schema,
                                            row,
                                            fieldTypeMap)
      }
    val nebulaNode = NebulaNode(values)
    nodes.append(nebulaNode)
    if (nodes.size >= nebulaOptions.batchSize) {
      execute()
    }
  }


  /**
   * submit buffer vertices to nebula
   */
  def execute(): Unit = {
    writeNodes(nodes)
    nodes.clear()
  }

  def writeNodes(vertices: ListBuffer[NebulaNode]): Unit = {
    val exec   = getGql(vertices.toList)
    val result = submit(exec)
    if (result.isSucceeded) {
      if (!nebulaOptions.disableWriteLog) {
        LOG.info(
          s"batch write for ${nebulaOptions.label} succeed. batch size(${vertices.size}), latency(${result.getLatency})")
      }
    } else {
      if (vertices.size == 1
        && result.getErrorCode != ErrorCode.LEADER_CHANGED
        && !result.getErrorCode.isRpcError
        && !result.getErrorCode.isRaftError) {
        if (nebulaOptions.errorWhenFailed) {
          throw new RuntimeException(s"write node ${nebulaOptions.label} failed: ${result.getErrorMessage}")
        } else {
          failedExecs.append(exec)
          LOG.error(s"write node ${nebulaOptions.label} failed: ${result.getErrorMessage}.")
          return
        }
      }
      // re-execute the vertices one by one
      LOG.warn(
        s"write node ${nebulaOptions.label} failed: ${result.getErrorMessage}, " +
          s"now retry writing one by one.")
      vertices.par.foreach { node => writeNode(node) }
    }
  }

  def writeNode(node: NebulaNode): Unit = {
    val exec   = getGql(List(node))
    val result = submit(exec)
    if (result.isSucceeded) {
      if (!nebulaOptions.disableWriteLog) {
        LOG.info(s"write ${nebulaOptions.label}, batch size(1), latency(${result.getLatency}ms)")
      }
      return
    }
    if (result.getErrorCode == ErrorCode.NODE_ALREADY_EXIST) {
      LOG.warn(
        s"write ${nebulaOptions.label} failed, the node already exists. ")
      return
    }
    // retry the write execution for RPC_ERROR(NN), LEADER_CHANGED(ND005), RAFT_ERROR(NA)
    var executeResult = result
    var retry         = 0
    while (retry < nebulaOptions.executionRetry &&
      (executeResult.getErrorCode.isRpcError
        || executeResult.getErrorCode == ErrorCode.LEADER_CHANGED
        || executeResult.getErrorCode.isRaftError)) {
      retry += 1
      Thread.sleep(nebulaOptions.executionRetryInterval)
      executeResult = submit(exec)
      if (executeResult.isSucceeded) {
        if (!nebulaOptions.disableWriteLog) {
          LOG.info(
            s"write node ${nebulaOptions.label}, batch size(1), latency(${executeResult.getLatency}ms)")
        }
        return
      }
    }
    if (nebulaOptions.errorWhenFailed) {
      throw new RuntimeException(s"write node ${nebulaOptions.label} failed: ${executeResult.getErrorMessage}")
    } else {
      LOG.error(s"write node ${nebulaOptions.label} failed: ${executeResult.getErrorMessage}.")
      failedExecs.append(exec)
    }
  }

  private def getGql(nebulaVertices: List[NebulaNode]): String = {
    val nebulaNodes = NebulaNodes(nebulaOptions.label, nebulaVertices, pkNames, fieldTypeMap)
    val exec        = nebulaOptions.writeMode match {
      case WriteMode.INSERT =>
        NebulaExecutor.toInsertSentence(nebulaOptions.graphName, nebulaNodes, "")
      case WriteMode.INSERTREPLACE =>
        NebulaExecutor.toInsertSentence(nebulaOptions.graphName, nebulaNodes, "OR REPLACE")
      case WriteMode.INSERTIGNORE =>
        NebulaExecutor.toInsertSentence(nebulaOptions.graphName, nebulaNodes, "OR IGNORE")
      case WriteMode.INSERTUPDATE =>
        NebulaExecutor.toInsertSentence(nebulaOptions.graphName, nebulaNodes, "OR UPDATE")
      case WriteMode.UPDATE =>
        NebulaExecutor.toUpdateSentence(nebulaOptions.graphName, nebulaOptions.label, nebulaNodes)
      case WriteMode.DELETE =>
        NebulaExecutor.toDeleteSentence(nebulaOptions.graphName, nebulaOptions.label, nebulaNodes, "DELETE")
      case WriteMode.DETACHDELETE =>
        NebulaExecutor.toDeleteSentence(nebulaOptions.graphName, nebulaOptions.label, nebulaNodes, "DETACH DELETE")
      case _ =>
        throw new IllegalArgumentException(s"write mode ${nebulaOptions.writeMode} not supported.")
    }
    exec
  }

  def abortWriter(): Unit = {
    LOG.error("insert node task abort.")
    graphProvider.close()
  }

}
