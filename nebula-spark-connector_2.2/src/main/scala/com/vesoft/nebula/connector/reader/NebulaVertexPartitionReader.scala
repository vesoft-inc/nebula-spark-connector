/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

package com.vesoft.nebula.connector.reader

import com.vesoft.nebula.connector.NebulaOptions
import org.apache.spark.Partition
import org.apache.spark.sql.types.StructType

class NebulaVertexPartitionReader(index: Partition,
                                  nebulaOptions: NebulaOptions,
                                  schema: StructType)
    extends NebulaIterator(index, nebulaOptions, schema) {

  override def hasNext: Boolean = hasNextVertexRow

}
