/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

package com.vesoft.nebula.connector.reader

import com.vesoft.nebula.connector.{NebulaOptions}
import org.apache.spark.sql.catalyst.InternalRow

import org.apache.spark.sql.sources.v2.reader.InputPartitionReader
import org.apache.spark.sql.types.StructType
import org.slf4j.{Logger, LoggerFactory}

/**
  * Read nebula data for each spark partition
  */
abstract class NebulaPartitionReader extends InputPartitionReader[InternalRow] with NebulaReader {
  private val LOG: Logger = LoggerFactory.getLogger(this.getClass)

  /**
    * @param index identifier for spark partition
    * @param nebulaOptions nebula Options
    * @param schema of data need to read
    */
  def this(index: Int, nebulaOptions: NebulaOptions, schema: StructType) {
    this()
    super.init(index, nebulaOptions, schema)
  }

  override def get(): InternalRow = super.getRow()

  override def close(): Unit = {
    super.closeReader()
  }
}
