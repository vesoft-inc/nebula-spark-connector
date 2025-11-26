/*
 * Copyright (c) 2025 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

package com.vesoft.nebula.spark.connector.writer

import com.vesoft.nebula.spark.common.writer.EdgeWriter
import com.vesoft.nebula.spark.common.NebulaOptions
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.sources.v2.writer.{DataWriter, WriterCommitMessage}
import org.apache.spark.sql.types.StructType

class NebulaEdgeWriter(nebulaOptions: NebulaOptions,
                       dfSrcPkFieldsIndex: List[Int],
                       dfDstPkFieldsIndex: List[Int],
                       schema: StructType)
  extends EdgeWriter(nebulaOptions, dfSrcPkFieldsIndex, dfDstPkFieldsIndex, schema)
    with DataWriter[InternalRow] {

  /**
   * write one edge record to buffer
   */
  override def write(row: InternalRow): Unit = {
    writeRow(row)
  }


  override def commit(): WriterCommitMessage = {
    if (edges.nonEmpty) {
      execute()
    }
    graphProvider.close()
    NebulaCommitMessage.apply(failedExecs.toList)
  }

  override def abort(): Unit = {
    abortWriter()
  }
}
