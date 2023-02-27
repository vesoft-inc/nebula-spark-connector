/* Copyright (c) 2022 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

package com.vesoft.nebula.connector.reader

import java.util

import com.vesoft.nebula.connector.NebulaOptions
import org.apache.spark.sql.connector.read.{
  Batch,
  InputPartition,
  PartitionReaderFactory,
  Scan,
  ScanBuilder,
  SupportsPushDownFilters,
  SupportsPushDownRequiredColumns
}
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types.StructType

import scala.collection.mutable.ListBuffer
import scala.jdk.CollectionConverters.asScalaBufferConverter

class SimpleScanBuilder(nebulaOptions: NebulaOptions, schema: StructType)
    extends ScanBuilder
    with SupportsPushDownFilters
    with SupportsPushDownRequiredColumns {

  private var filters: Array[Filter] = Array[Filter]()

  override def build(): Scan = {
    new SimpleScan(nebulaOptions, nebulaOptions.partitionNums.toInt, schema)
  }

  override def pushFilters(pushFilters: Array[Filter]): Array[Filter] = {
    if (nebulaOptions.pushDownFiltersEnabled) {
      filters = pushFilters
    }
    pushFilters
  }

  override def pushedFilters(): Array[Filter] = filters

  override def pruneColumns(requiredColumns: StructType): Unit = {
    if (!nebulaOptions.pushDownFiltersEnabled || requiredColumns == schema) {
      new StructType()
    }
  }
}

class SimpleScan(nebulaOptions: NebulaOptions, nebulaTotalPart: Int, schema: StructType)
    extends Scan
    with Batch {
  override def toBatch: Batch = this

  override def planInputPartitions(): Array[InputPartition] = {
    val partitionSize = nebulaTotalPart
    val inputPartitions = for (i <- 1 to partitionSize)
      yield {
        NebulaPartition(i)
      }
    inputPartitions.map(_.asInstanceOf[InputPartition]).toArray
  }

  override def readSchema(): StructType = schema

  override def createReaderFactory(): PartitionReaderFactory =
    new NebulaPartitionReaderFactory(nebulaOptions, schema)
}

/**
  * An identifier for a partition in an NebulaRDD.
  */
case class NebulaPartition(partition: Int) extends InputPartition
