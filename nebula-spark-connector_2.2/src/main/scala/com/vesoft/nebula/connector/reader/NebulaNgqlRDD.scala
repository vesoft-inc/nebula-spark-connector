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

import scala.collection.mutable.ListBuffer

class NebulaNgqlRDD(val sqlContext: SQLContext,
                    var nebulaOptions: NebulaOptions,
                    schema: StructType)
    extends RDD[InternalRow](sqlContext.sparkContext, Nil) {

  /**
    * start to get edge data from query resultSet
    *
    * @param split
    * @param context
    * @return Iterator<InternalRow>
    */
  override def compute(split: Partition, context: TaskContext): Iterator[InternalRow] = {
    new NebulaNgqlEdgeReader()
  }

  override def getPartitions: Array[Partition] = {
    val partitions = new Array[Partition](1)
    partitions(0) = NebulaNgqlPartition(0)
    partitions
  }

}

/**
  * An identifier for a partition in an NebulaRDD.
  */
case class NebulaNgqlPartition(indexNum: Int) extends Partition {
  override def index: Int = indexNum
}
