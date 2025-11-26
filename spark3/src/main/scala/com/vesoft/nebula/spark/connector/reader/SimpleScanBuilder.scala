/*
 * Copyright (c) 2025 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */
package com.vesoft.nebula.spark.connector.reader

import com.vesoft.nebula.spark.common.{DataTypeEnum, NebulaOptions, NebulaUtils}
import org.apache.spark.sql.connector.read._
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types.StructType
import org.slf4j.LoggerFactory

class SimpleScanBuilder(nebulaOptions: NebulaOptions)
  extends ScanBuilder
    with SupportsPushDownFilters
    with SupportsPushDownRequiredColumns {

  private val filters: Array[Filter] = Array[Filter]()

  override def build(): Scan = {
    new SimpleScan(nebulaOptions)
  }

  override def pushFilters(pushFilters: Array[Filter]): Array[Filter] = {
    pushFilters
  }

  override def pushedFilters(): Array[Filter] = filters

  override def pruneColumns(requiredColumns: StructType): Unit = {
    new StructType()
  }
}

class SimpleScan(nebulaOptions: NebulaOptions)
  extends Scan
    with Batch {
  private val LOG                       = LoggerFactory.getLogger(this.getClass)
  private var datasetSchema: StructType = _

  override def toBatch: Batch = this

  override def planInputPartitions(): Array[InputPartition] = {
    val partitionNum = nebulaOptions.partitionNums
    val partitions   = for (i <- 1 to partitionNum)
      yield {
        NebulaPartition(i)
      }
    partitions.map(_.asInstanceOf[InputPartition]).toArray
  }

  override def readSchema(): StructType = {
    if (datasetSchema == null) {
      if (DataTypeEnum.NODE == DataTypeEnum.withName(nebulaOptions.dataType) || DataTypeEnum.EDGE == DataTypeEnum.withName(nebulaOptions.dataType)) {
        datasetSchema = NebulaUtils.getSchema(nebulaOptions)
      } else {
        datasetSchema = NebulaUtils.getSchemaForGql(nebulaOptions)
      }

      LOG.info(s"dataset's schema: $datasetSchema")
    }
    datasetSchema
  }

  override def createReaderFactory(): PartitionReaderFactory =
    new NebulaPartitionReaderFactory(nebulaOptions, datasetSchema)
}

/**
 * An identifier for a partition in an NebulaRDD.
 */
case class NebulaPartition(partition: Int) extends InputPartition
