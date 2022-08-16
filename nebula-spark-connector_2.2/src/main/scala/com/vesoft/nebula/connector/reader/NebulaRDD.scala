/* Copyright (c) 2022 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

package com.vesoft.nebula.connector.reader

import org.apache.spark.{Partition, TaskContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.catalyst.InternalRow

/**
  * @todo
  */
class NebulaRDD(val sqlContext: SQLContext) extends RDD[InternalRow](sqlContext.sparkContext, Nil) {

  /**
    * @todo
    * scan nebula data
    */
  override def compute(split: Partition, context: TaskContext): Iterator[InternalRow] = {
    null
  }

  /**
    * @todo
    * compute the nebula parts for each spark partition
    * */
  override def getPartitions: Array[Partition] = {
    null
  }
}
