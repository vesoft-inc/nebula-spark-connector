/*
 * Copyright (c) 2025 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */
package com.vesoft.nebula.spark.connector.writer

import com.vesoft.nebula.spark.common.{NebulaOptions, NebulaUtils}
import org.apache.spark.TaskContext
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.write.{BatchWrite, DataWriter, DataWriterFactory, PhysicalWriteInfo, WriterCommitMessage}
import org.apache.spark.sql.types.StructType
import org.slf4j.LoggerFactory

/**
  * creating and initializing the actual Nebula vertex writer at executor side
  */
class NebulaNodeWriterFactory(nebulaOptions: NebulaOptions, schema: StructType)
    extends DataWriterFactory {
  override def createWriter(partitionId: Int, taskId: Long): DataWriter[InternalRow] = {
    // check if the dataFrame's schema matches NebulaGraph's schema
    NebulaUtils.checkNodeSchemaMatchWithDataFrame(nebulaOptions, schema)
    new NebulaNodeWriter(nebulaOptions, schema)
  }
}

/**
  * creating and initializing the actual Nebula edge writer at executor side
  */
class NebulaEdgeWriterFactory(nebulaOptions: NebulaOptions,
                              srcIndices: List[Int],
                              dstIndices: List[Int],
                              schema: StructType)
    extends DataWriterFactory {
  override def createWriter(partitionId: Int, taskId: Long): DataWriter[InternalRow] = {
    // check if the dataFrame's schema matches NebulaGraph's schema
    NebulaUtils.checkEdgeSchemaMatchWithDataFrame(nebulaOptions, schema)
    new NebulaEdgeWriter(nebulaOptions, srcIndices, dstIndices, schema)
  }
}

/**
  * nebula vertex writer to create factory
  */
class NebulaDataSourceNodeWriter(nebulaOptions: NebulaOptions,
                                 schema: StructType)
    extends BatchWrite {
  private val LOG = LoggerFactory.getLogger(this.getClass)

  override def createBatchWriterFactory(info: PhysicalWriteInfo): DataWriterFactory = {
    new NebulaNodeWriterFactory(nebulaOptions, schema)
  }

  override def commit(messages: Array[WriterCommitMessage]): Unit = {
    LOG.debug(s"${messages.length}")
    for (msg <- messages) {
      val nebulaMsg = msg.asInstanceOf[NebulaCommitMessage]
      if (nebulaMsg.executeStatements.nonEmpty) {
        LOG.error(s"failed execs:\n ${nebulaMsg.executeStatements.toString()}")
      } else {
        LOG.info(s"execs for spark partition ${TaskContext.getPartitionId()} all succeed")
      }
    }
  }

  override def abort(messages: Array[WriterCommitMessage]): Unit = {
    LOG.error("NebulaDataSourceNodeWriter abort")
  }
}

/**
  * nebula edge writer to create factory
  */
class NebulaDataSourceEdgeWriter(nebulaOptions: NebulaOptions,
                                 srcIndices: List[Int],
                                 dstIndices: List[Int],
                                 schema: StructType)
    extends BatchWrite {
  private val LOG = LoggerFactory.getLogger(this.getClass)

  override def createBatchWriterFactory(info: PhysicalWriteInfo): DataWriterFactory = {
    new NebulaEdgeWriterFactory(nebulaOptions, srcIndices, dstIndices, schema)
  }

  override def commit(messages: Array[WriterCommitMessage]): Unit = {
    LOG.debug(s"${messages.length}")
    for (msg <- messages) {
      val nebulaMsg = msg.asInstanceOf[NebulaCommitMessage]
      if (nebulaMsg.executeStatements.nonEmpty) {
        LOG.error(s"failed execs:\n ${nebulaMsg.executeStatements.toString()}")
      } else {
        LOG.info(s"execs for spark partition ${TaskContext.getPartitionId()} all succeed")
      }
    }

  }

  override def abort(messages: Array[WriterCommitMessage]): Unit = {
    LOG.error("NebulaDataSourceEdgeWriter abort")
  }
}
