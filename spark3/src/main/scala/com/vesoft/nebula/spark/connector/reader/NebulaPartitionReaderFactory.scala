/*
 * Copyright (c) 2025 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */
package com.vesoft.nebula.spark.connector.reader

import com.vesoft.nebula.spark.common.{DataTypeEnum, NebulaOptions}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.read.{InputPartition, PartitionReader, PartitionReaderFactory}
import org.apache.spark.sql.types.StructType

class NebulaPartitionReaderFactory(private val nebulaOptions: NebulaOptions,
                                   private val schema: StructType)
  extends PartitionReaderFactory {
  override def createReader(inputPartition: InputPartition): PartitionReader[InternalRow] = {
    val partition = inputPartition.asInstanceOf[NebulaPartition].partition
    if (DataTypeEnum.NODE.toString.equals(nebulaOptions.dataType)) {
      new NebulaNodePartitionReader(partition, nebulaOptions, schema)
    } else if (DataTypeEnum.EDGE.toString.equals(nebulaOptions.dataType)) {
      new NebulaEdgePartitionReader(partition, nebulaOptions, schema)
    } else {
      new NebulaGqlPartitionReader(nebulaOptions)
    }
  }
}
