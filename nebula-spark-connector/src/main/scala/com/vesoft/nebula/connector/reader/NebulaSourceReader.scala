/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

package com.vesoft.nebula.connector.reader

import java.util

import com.vesoft.nebula.connector.{NebulaOptions, NebulaUtils}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.sources.v2.reader.{DataSourceReader, InputPartition}
import org.apache.spark.sql.types.{StructType}
import org.slf4j.LoggerFactory

import scala.collection.JavaConverters._

/**
  * Base class of Nebula Source Reader
  */
abstract class NebulaSourceReader(nebulaOptions: NebulaOptions) extends DataSourceReader {
  private val LOG = LoggerFactory.getLogger(this.getClass)

  private var datasetSchema: StructType = _

  override def readSchema(): StructType = {
    if (datasetSchema == null) {
      datasetSchema = NebulaUtils.getSchema(nebulaOptions)
    }

    LOG.info(s"dataset's schema: $datasetSchema")
    datasetSchema
  }

  protected def getSchema: StructType =
    if (datasetSchema == null) NebulaUtils.getSchema(nebulaOptions) else datasetSchema
}

/**
  * DataSourceReader for Nebula Vertex
  */
class NebulaDataSourceVertexReader(nebulaOptions: NebulaOptions)
    extends NebulaSourceReader(nebulaOptions) {

  override def planInputPartitions(): util.List[InputPartition[InternalRow]] = {
    val partitionNum = nebulaOptions.partitionNums.toInt
    val partitions = for (index <- 1 to partitionNum)
      yield {
        new NebulaVertexPartition(index, nebulaOptions, getSchema)
      }
    partitions.map(_.asInstanceOf[InputPartition[InternalRow]]).asJava
  }
}

/**
  * DataSourceReader for Nebula Edge
  */
class NebulaDataSourceEdgeReader(nebulaOptions: NebulaOptions)
    extends NebulaSourceReader(nebulaOptions) {

  override def planInputPartitions(): util.List[InputPartition[InternalRow]] = {
    val partitionNum = nebulaOptions.partitionNums.toInt
    val partitions = for (index <- 1 to partitionNum)
      yield new NebulaEdgePartition(index, nebulaOptions, getSchema)

    partitions.map(_.asInstanceOf[InputPartition[InternalRow]]).asJava
  }
}

/**
  * DataSourceReader for Nebula Edge by ngql
  */
class NebulaDataSourceNgqlEdgeReader(nebulaOptions: NebulaOptions)
    extends NebulaSourceReader(nebulaOptions) {

  override def planInputPartitions(): util.List[InputPartition[InternalRow]] = {
    val partitions = new util.ArrayList[InputPartition[InternalRow]]()
    partitions.add(new NebulaNgqlEdgePartition(nebulaOptions, getSchema))
    partitions
  }
}
