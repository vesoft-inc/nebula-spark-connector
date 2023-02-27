/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

package com.vesoft.nebula.connector.reader

import com.vesoft.nebula.connector.{NebulaOptions, PartitionUtils}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.read.PartitionReader
import org.apache.spark.sql.types.StructType
import org.slf4j.{Logger, LoggerFactory}

/**
  * Read nebula data for each spark partition
  */
abstract class NebulaPartitionReader extends PartitionReader[InternalRow] with NebulaReader {
  private val LOG: Logger = LoggerFactory.getLogger(this.getClass)

  /**
    * @param index identifier for spark partition
    * @param nebulaOptions nebula Options
    * @param schema of data need to read
    */
  def this(index: Int, nebulaOptions: NebulaOptions, schema: StructType) {
    this()
    val totalPart = super.init(index, nebulaOptions, schema)
    // index starts with 1
    val scanParts = PartitionUtils.getScanParts(index, totalPart, nebulaOptions.partitionNums.toInt)
    LOG.info(s"partition index: ${index}, scanParts: ${scanParts.toString}")
    scanPartIterator = scanParts.iterator
  }

  override def get(): InternalRow = super.getRow()

  override def close(): Unit = {
    super.closeReader()
  }
}
