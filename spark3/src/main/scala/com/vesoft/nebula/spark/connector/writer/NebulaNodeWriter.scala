/*
 * Copyright (c) 2025 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */
package com.vesoft.nebula.spark.connector.writer

import com.vesoft.nebula.spark.common.NebulaOptions
import com.vesoft.nebula.spark.common.writer.NodeWriter
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.write.{DataWriter, WriterCommitMessage}
import org.apache.spark.sql.types.StructType

class NebulaNodeWriter(nebulaOptions: NebulaOptions, schema: StructType)
  extends NodeWriter(nebulaOptions, schema)
    with DataWriter[InternalRow] {

  /**
   * write one vertex row to buffer
   */
  override def write(row: InternalRow): Unit = {
    writeRow(row)
  }

  override def commit(): WriterCommitMessage = {
    if (nodes.nonEmpty) {
      execute()
    }
    graphProvider.close()
    NebulaCommitMessage(failedExecs.toList)
  }

  override def abort(): Unit = {
    abortWriter()
  }

  override def close(): Unit = {
    graphProvider.close()
  }
}
