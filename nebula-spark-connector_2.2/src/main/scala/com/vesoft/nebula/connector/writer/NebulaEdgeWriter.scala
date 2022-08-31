/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

package com.vesoft.nebula.connector.writer

import com.vesoft.nebula.connector.{KeyPolicy, NebulaEdge, NebulaEdges, NebulaOptions, WriteMode}
import org.apache.spark.TaskContext
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types.StructType
import org.slf4j.LoggerFactory

import scala.collection.mutable.ListBuffer

class NebulaEdgeWriter(nebulaOptions: NebulaOptions,
                       srcIndex: Int,
                       dstIndex: Int,
                       rankIndex: Option[Int],
                       schema: StructType)
    extends NebulaWriter(nebulaOptions, schema) {

  private val LOG = LoggerFactory.getLogger(this.getClass)

  val rankIdx = if (rankIndex.isDefined) rankIndex.get else -1
  val propNames = NebulaExecutor.assignEdgePropNames(schema,
                                                     srcIndex,
                                                     dstIndex,
                                                     rankIdx,
                                                     nebulaOptions.srcAsProp,
                                                     nebulaOptions.dstAsProp,
                                                     nebulaOptions.rankAsProp)
  val fieldTypMap: Map[String, Integer] =
    if (nebulaOptions.writeMode == WriteMode.DELETE) Map[String, Integer]()
    else metaProvider.getEdgeSchema(nebulaOptions.spaceName, nebulaOptions.label)

  val srcPolicy =
    if (nebulaOptions.srcPolicy.isEmpty) Option.empty
    else Option(KeyPolicy.withName(nebulaOptions.srcPolicy))
  val dstPolicy = {
    if (nebulaOptions.dstPolicy.isEmpty) Option.empty
    else Option(KeyPolicy.withName(nebulaOptions.dstPolicy))
  }

  /** buffer to save batch edges */
  var edges: ListBuffer[NebulaEdge] = new ListBuffer()

  prepareSpace()

  /**
    * write one edge record to buffer
    */
  override def write(row: InternalRow): Unit = {
    val srcId = NebulaExecutor.extraID(schema, row, srcIndex, srcPolicy, isVidStringType)
    val dstId = NebulaExecutor.extraID(schema, row, dstIndex, dstPolicy, isVidStringType)
    val rank =
      if (rankIndex.isEmpty) Option.empty
      else Option(NebulaExecutor.extraRank(schema, row, rankIndex.get))
    val values =
      if (nebulaOptions.writeMode == WriteMode.DELETE) List()
      else
        NebulaExecutor.assignEdgeValues(schema,
                                        row,
                                        srcIndex,
                                        dstIndex,
                                        rankIdx,
                                        nebulaOptions.srcAsProp,
                                        nebulaOptions.dstAsProp,
                                        nebulaOptions.rankAsProp,
                                        fieldTypMap)
    val nebulaEdge = NebulaEdge(srcId, dstId, rank, values)
    edges.append(nebulaEdge)
    if (edges.size >= nebulaOptions.batch) {
      execute()
    }
  }

  /**
    * submit buffer edges to nebula
    */
  def execute(): Unit = {
    val nebulaEdges = NebulaEdges(propNames, edges.toList, srcPolicy, dstPolicy)
    val exec = nebulaOptions.writeMode match {
      case WriteMode.INSERT => NebulaExecutor.toExecuteSentence(nebulaOptions.label, nebulaEdges)
      case WriteMode.UPDATE =>
        NebulaExecutor.toUpdateExecuteStatement(nebulaOptions.label, nebulaEdges)
      case WriteMode.DELETE =>
        NebulaExecutor.toDeleteExecuteStatement(nebulaOptions.label, nebulaEdges)
      case _ =>
        throw new IllegalArgumentException(s"write mode ${nebulaOptions.writeMode} not supported.")
    }
    edges.clear()
    submit(exec)
  }

  override def writeData(): (TaskContext, Iterator[Row]) => NebulaCommitMessage =
    (context, iterRow) => {
      while (iterRow.hasNext) {
        val internalRow = rowEncoder.toRow(iterRow.next())
        write(internalRow)
      }
      if (edges.nonEmpty) {
        execute()
      }
      graphProvider.close()
      NebulaCommitMessage(TaskContext.getPartitionId(), failedExecs.toList)
    }
}
