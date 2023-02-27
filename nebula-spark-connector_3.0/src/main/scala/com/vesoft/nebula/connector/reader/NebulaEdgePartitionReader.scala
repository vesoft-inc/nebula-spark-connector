/* Copyright (c) 2022 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

package com.vesoft.nebula.connector.reader

import com.vesoft.nebula.connector.NebulaOptions
import org.apache.spark.sql.types.StructType

class NebulaEdgePartitionReader(index: Int, nebulaOptions: NebulaOptions, schema: StructType)
    extends NebulaPartitionReader(index, nebulaOptions, schema) {

  override def next(): Boolean = hasNextEdgeRow
}
