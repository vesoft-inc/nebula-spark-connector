/*
 * Copyright (c) 2025 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */
package com.vesoft.nebula.spark.common.writer

import com.vesoft.nebula.driver.graph.ErrorCode
import com.vesoft.nebula.spark.common.nebula.EdgeDesc
import com.vesoft.nebula.spark.common.{NebulaEdge, NebulaEdges, NebulaOptions, WriteMode}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types.StructType

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

class EdgeWriter(nebulaOptions: NebulaOptions,
                 dfSrcPkFieldsIndex: List[Int],
                 dfDstPkFieldsIndex: List[Int],
                 schema: StructType) extends NebulaWriter(nebulaOptions) {

  protected val edgeDesc: EdgeDesc = graphProvider.getEdgeDesc(nebulaOptions.graphName, nebulaOptions.label)

  val fieldTypeMap: Map[String, String] = edgeDesc.properties

  /** buffer to save batch edges */
  var edges: ListBuffer[NebulaEdge] = new ListBuffer()


  def writeRow(row:InternalRow): Unit = {
    val srcIds: mutable.HashMap[String, String] = new mutable.HashMap[String, String]()
    for (i <- dfSrcPkFieldsIndex.indices) {
      val srcIdValue = NebulaExecutor.extraPropValue(row, schema, dfSrcPkFieldsIndex(i), edgeDesc.srcNodePkDataTypeMap(edgeDesc.srcNodePkNames(i)))
      if (srcIdValue == null) {
        LOG.error(s">>>> record has null value at index ${dfSrcPkFieldsIndex(i)} for primary key, ignore it. record:$row")
        return
      }
      srcIds.put(edgeDesc.srcNodePkNames(i), srcIdValue)
    }

    val dstIds: mutable.HashMap[String, String] = new mutable.HashMap[String, String]()
    for (i <- dfDstPkFieldsIndex.indices) {
      val dstIdValue = NebulaExecutor.extraPropValue(row, schema, dfDstPkFieldsIndex(i), edgeDesc.dstNodePkDataTypeMap(edgeDesc.dstNodePkNames(i)))
      if (dstIdValue == null) {
        LOG.error(s">>>> record has null value at index ${dfDstPkFieldsIndex(i)} for primary key, ignore it. record:$row")
        return
      }
      dstIds.put(edgeDesc.dstNodePkNames(i), dstIdValue)
    }

    val values     =
      if (nebulaOptions.writeMode == WriteMode.DELETE) {
        // delete mode does not need property.
        Map[String, String]()
      } else {
        NebulaExecutor.assignEdgeValues(schema,
                                        row,
                                        dfSrcPkFieldsIndex,
                                        dfDstPkFieldsIndex,
                                        nebulaOptions.srcPksAsProp,
                                        nebulaOptions.dstPksAsProp,
                                        fieldTypeMap)
      }
    val nebulaEdge = NebulaEdge(srcIds.toMap, dstIds.toMap, values)
    edges.append(nebulaEdge)
    if (edges.size >= nebulaOptions.batchSize) {
      execute()
    }
  }

  /**
   * submit buffer edges to nebula
   */
  def execute(): Unit = {
    writeEdges(edges)
    edges.clear()
  }

  def writeEdges(edges: ListBuffer[NebulaEdge]): Unit = {
    val exec   = getGql(edges.toList)
    val result = submit(exec)
    if (result.isSucceeded) {
      if (!nebulaOptions.disableWriteLog) {
        LOG.info(
          s"batch write for ${nebulaOptions.label} succeed. batch size(${edges.size}), latency(${result.getLatency})")
      }
    } else {
      if (edges.size == 1
        && result.getErrorCode != ErrorCode.LEADER_CHANGED
        && !result.getErrorCode.isRpcError
        && !result.getErrorCode.isRaftError) {
        failedExecs.append(exec)
        LOG.error(s"write edge ${nebulaOptions.label} failed: ${result.getErrorMessage}.")
        return
      }
      // re-execute the vertices one by one
      LOG.warn(
        s"write edge ${nebulaOptions.label} failed: ${result.getErrorMessage}, " +
          s"now retry writing one by one.")
      edges.par.foreach { edge => writeEdge(edge) }
    }
  }

  def writeEdge(edge: NebulaEdge): Unit = {
    val exec   = getGql(List(edge))
    val result = submit(exec)
    if (result.isSucceeded) {
      if (!nebulaOptions.disableWriteLog) {
        LOG.info(s"write ${nebulaOptions.label}, batch size(1), latency(${result.getLatency}ms)")
      }
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
            s"write edge ${nebulaOptions.label}, batch size(1), latency(${executeResult.getLatency}ms)")
        }
        return
      }
    }
    LOG.error(s"write edge ${nebulaOptions.label} failed: ${executeResult.getErrorMessage}.")
    failedExecs.append(exec)
  }

  private def getGql(edges: List[NebulaEdge]): String = {
    val nebulaEdges = NebulaEdges(
      nebulaOptions.label,
      edgeDesc.srcNodeTypeName,
      edgeDesc.srcNodePkNames,
      edgeDesc.srcNodePkDataTypeMap,
      nebulaOptions.srcPkFields,
      edgeDesc.dstNodeTypeName,
      edgeDesc.dstNodePkNames,
      edgeDesc.dstNodePkDataTypeMap,
      nebulaOptions.dstPkFields,
      edges,
      fieldTypeMap)
    val exec        = nebulaOptions.writeMode match {
      case WriteMode.INSERT =>
        NebulaExecutor.toInsertSentence(nebulaOptions.graphName, nebulaEdges, "")
      case WriteMode.INSERTREPLACE =>
        NebulaExecutor.toInsertSentence(nebulaOptions.graphName, nebulaEdges, "OR REPLACE")
      case WriteMode.INSERTIGNORE =>
        NebulaExecutor.toInsertSentence(nebulaOptions.graphName, nebulaEdges, "OR IGNORE")
      case WriteMode.INSERTUPDATE =>
        NebulaExecutor.toInsertSentence(nebulaOptions.graphName, nebulaEdges, "OR UPDATE")
      case WriteMode.UPDATE =>
        NebulaExecutor.toUpdateSentence(nebulaOptions.graphName, nebulaOptions.label, nebulaEdges)
      case WriteMode.DELETE | WriteMode.DETACHDELETE =>
        NebulaExecutor.toDeleteSentence(nebulaOptions.graphName, nebulaOptions.label, nebulaEdges)
      case _ =>
        throw new IllegalArgumentException(s"write mode ${nebulaOptions.writeMode} not supported.")
    }
    exec
  }


  def abortWriter(): Unit = {
    LOG.error("insert edge task abort.")
    graphProvider.close()
  }
}


