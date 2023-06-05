/* Copyright (c) 2022 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

package com.vesoft.nebula.connector.reader

import com.vesoft.nebula.connector.{DataTypeEnum, NebulaOptions}
import org.apache.spark.{Partition, TaskContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types.StructType

class NebulaRDD(val sqlContext: SQLContext, var nebulaOptions: NebulaOptions, schema: StructType)
    extends RDD[InternalRow](sqlContext.sparkContext, Nil) {

  /**
    * start to scan vertex or edge data
    *
    * @param split
    * @param context
    * @return Iterator<InternalRow>
    */
  override def compute(split: Partition, context: TaskContext): Iterator[InternalRow] = {
    val dataType = nebulaOptions.dataType
    if (DataTypeEnum.VERTEX.toString.equalsIgnoreCase(dataType))
      new NebulaVertexPartitionReader(split, nebulaOptions, schema)
    else new NebulaEdgePartitionReader(split, nebulaOptions, schema)
  }

  override def getPartitions = {
    val partitionNumber = nebulaOptions.partitionNums.toInt
    val partitions      = new Array[Partition](partitionNumber)
    for (i <- 0 until partitionNumber) {
      partitions(i) = NebulaPartition(i)
    }
    partitions
  }
}

/**
  * An identifier for a partition in an NebulaRDD.
  */
case class NebulaPartition(indexNum: Int) extends Partition {
  override def index: Int = indexNum

  /**
    * allocate scanPart to partition
    *
    * @param totalPart nebula data part num
    * @return scan data part list
    */
  def getScanParts(totalPart: Int, totalPartition: Int): List[Int] =
    (indexNum + 1 to totalPart by totalPartition).toList

}
