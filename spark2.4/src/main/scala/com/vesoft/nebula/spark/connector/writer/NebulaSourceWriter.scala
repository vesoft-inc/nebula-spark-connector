/*
 * Copyright (c) 2025 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

package com.vesoft.nebula.spark.connector.writer

import com.vesoft.nebula.spark.common.{NebulaOptions, NebulaUtils}
import com.vesoft.nebula.spark.common.nebula.{GraphProvider, NodeDesc}
import org.apache.spark.TaskContext
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.sources.v2.writer.{DataSourceWriter, DataWriter, DataWriterFactory, WriterCommitMessage}
import org.apache.spark.sql.types.StructType
import org.slf4j.LoggerFactory

import scala.collection.mutable.ListBuffer

/**
 * creating and initializing the actual Nebula node writer at executor side
 */
class NebulaNodeWriterFactory(nebulaOptions: NebulaOptions, schema: StructType)
  extends DataWriterFactory[InternalRow] {
  override def createDataWriter(partitionId: Int,
                                taskId: Long,
                                epochId: Long): DataWriter[InternalRow] = {
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
  extends DataWriterFactory[InternalRow] {
  override def createDataWriter(partitionId: Int,
                                taskId: Long,
                                epochId: Long): DataWriter[InternalRow] = {
    // check if the dataFrame's schema matches NebulaGraph's schema
    NebulaUtils.checkEdgeSchemaMatchWithDataFrame(nebulaOptions, schema)
    new NebulaEdgeWriter(nebulaOptions, srcIndices, dstIndices, schema)
  }
}

/**
 * nebula node writer to create factory
 */
class NebulaDataSourceNodeWriter(nebulaOptions: NebulaOptions,
                                 schema: StructType)
  extends DataSourceWriter {
  private val LOG = LoggerFactory.getLogger(this.getClass)

  override def createWriterFactory(): DataWriterFactory[InternalRow] = {
    // check if the dataFrame's schema matches NebulaGraph's schema
    val graphProvider      = new GraphProvider(nebulaOptions)
    var nodeDesc: NodeDesc = null
    try {
      nodeDesc = graphProvider.getNodeDesc(nebulaOptions.graphName, nebulaOptions.label)
    } finally {
      graphProvider.close()
    }
    // check primary key name exists in dataframe's schema
    val dataFrameFields = new ListBuffer[String]
    schema.fields.toList.foreach(field => dataFrameFields.append(field.name))
    nodeDesc.nodePkNames.foreach(pk => {
      assert(dataFrameFields.contains(pk), s"the dataframe does not contain the node primary key property ${pk}")

    })
    for (field <- dataFrameFields) {
      assert(nodeDesc.properties.keySet.contains(field),
             s"the dataframe field $field does not match NebulaGraph node ${nebulaOptions.label} properties.")
    }
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
  extends DataSourceWriter {
  private val LOG = LoggerFactory.getLogger(this.getClass)

  override def createWriterFactory(): DataWriterFactory[InternalRow] = {
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
