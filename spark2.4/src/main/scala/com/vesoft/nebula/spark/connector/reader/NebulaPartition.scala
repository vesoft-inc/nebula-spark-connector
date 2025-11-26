/*
 * Copyright (c) 2025 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */
package com.vesoft.nebula.spark.connector.reader

import com.vesoft.nebula.spark.common.NebulaOptions
import com.vesoft.nebula.spark.common.reader.NebulaGqlReader
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.sources.v2.reader.{InputPartition, InputPartitionReader}
import org.apache.spark.sql.types.StructType

class NebulaNodePartition(index: Int, nebulaOptions: NebulaOptions, schema: StructType)
    extends InputPartition[InternalRow] {
  override def createPartitionReader(): InputPartitionReader[InternalRow] =
    new NebulaNodePartitionReader(index, nebulaOptions, schema)
}

class NebulaEdgePartition(index: Int, nebulaOptions: NebulaOptions, schema: StructType)
    extends InputPartition[InternalRow] {
  override def createPartitionReader(): InputPartitionReader[InternalRow] =
    new NebulaEdgePartitionReader(index, nebulaOptions, schema)
}

class NebulaGqlPartition(nebulaOptions: NebulaOptions) extends InputPartition[InternalRow]{
  override def createPartitionReader(): InputPartitionReader[InternalRow] =
    new NebulaGqlPartitionReader(nebulaOptions)
}
