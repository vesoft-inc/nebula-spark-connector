/* Copyright (c) 2022 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

package com.vesoft.nebula.connector.reader

import com.vesoft.nebula.connector.{DataTypeEnum, NebulaOptions}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.read.{InputPartition, PartitionReader, PartitionReaderFactory}
import org.apache.spark.sql.types.StructType

class NebulaPartitionReaderFactory(private val nebulaOptions: NebulaOptions,
                                   private val schema: StructType)
    extends PartitionReaderFactory {
  override def createReader(inputPartition: InputPartition): PartitionReader[InternalRow] = {
    val partition = inputPartition.asInstanceOf[NebulaPartition].partition
    if (DataTypeEnum.VERTEX.toString.equals(nebulaOptions.dataType)) {

      new NebulaVertexPartitionReader(partition, nebulaOptions, schema)
    } else {
      new NebulaEdgePartitionReader(partition, nebulaOptions, schema)
    }
  }
}
