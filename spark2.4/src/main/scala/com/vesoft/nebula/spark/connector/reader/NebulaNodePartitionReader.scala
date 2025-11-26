/*
 * Copyright (c) 2025 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */
package com.vesoft.nebula.spark.connector.reader

import com.vesoft.nebula.spark.common.NebulaOptions
import org.apache.spark.sql.types.StructType

class NebulaNodePartitionReader(index: Int, nebulaOptions: NebulaOptions, schema: StructType)
    extends NebulaPartitionReader(index, nebulaOptions, schema) {

  override def next(): Boolean = hasNextNodeRow
}
