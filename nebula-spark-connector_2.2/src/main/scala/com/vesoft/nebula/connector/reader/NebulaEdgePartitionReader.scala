/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

package com.vesoft.nebula.connector.reader

import com.vesoft.nebula.connector.NebulaOptions
import org.apache.spark.Partition
import org.apache.spark.sql.types.StructType
import org.slf4j.{Logger, LoggerFactory}

class NebulaEdgePartitionReader(index: Partition, nebulaOptions: NebulaOptions, schema: StructType)
    extends NebulaIterator(index, nebulaOptions, schema) {
  private val LOG: Logger = LoggerFactory.getLogger(this.getClass)

  override def hasNext(): Boolean = hasNextEdgeRow
}
