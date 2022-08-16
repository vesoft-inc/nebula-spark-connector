/* Copyright (c) 2022 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

package com.vesoft.nebula.connector.reader

import org.apache.spark.Partition

/**
  * @todo
  * An identifier for a partition in an NebulaRDD.
  */
case class NebulaPartition(indexNum: Int) extends Partition {
  override def index: Int = indexNum

  /**
    * @todo
    * allocate scanPart to each spark partition
    *
    * @param totalPart nebula data part num
    * @return scan data part list
    */
  def getScanParts(totalPart: Int, totalPartition: Int): List[Integer] = {
    null
  }
}
