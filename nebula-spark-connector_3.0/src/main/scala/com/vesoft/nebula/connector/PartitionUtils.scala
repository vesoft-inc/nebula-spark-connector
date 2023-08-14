/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

package com.vesoft.nebula.connector

object PartitionUtils {

  /**
    * compute each spark partition should assign how many nebula parts
    *
    * @param index spark partition index
    * @param nebulaTotalPart nebula space partition number
    * @param sparkPartitionNum spark total partition number
    * @return the list of nebula partitions assign to spark index partition
    */
  def getScanParts(index: Int, nebulaTotalPart: Int, sparkPartitionNum: Int): List[Int] =
    (index to nebulaTotalPart by sparkPartitionNum).toList

}
