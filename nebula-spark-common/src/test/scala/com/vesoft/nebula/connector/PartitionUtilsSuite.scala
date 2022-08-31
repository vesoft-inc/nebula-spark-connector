/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

package com.vesoft.nebula.connector

import org.scalatest.funsuite.AnyFunSuite

/**
  * base data: spark partition is 10
  */
class PartitionUtilsSuite extends AnyFunSuite {
  val partition: Int = 10

  test("getScanParts: nebula part is the same with spark partition") {
    val nebulaPart: Int = 10
    for (i <- 1 to 10) {
      val partsForIndex = PartitionUtils.getScanParts(i, nebulaPart, partition)
      assert(partsForIndex.size == 1)
      assert(partsForIndex.head == i)
    }
  }

  test("getScanParts: nebula part is more than spark partition") {
    val nebulaPart: Int = 20
    for (i <- 1 to 10) {
      val partsForIndex = PartitionUtils.getScanParts(i, nebulaPart, partition)
      assert(partsForIndex.contains(i) && partsForIndex.contains(i + 10))
      assert(partsForIndex.size == 2)
    }
  }

  test("getScanParts: nebula part is less than spark partition") {
    val nebulaPart: Int = 5
    for (i <- 1 to 5) {
      val partsForIndex = PartitionUtils.getScanParts(i, nebulaPart, partition)
      assert(partsForIndex.contains(i))
    }
    for (j <- 6 to 10) {
      val partsForIndex = PartitionUtils.getScanParts(j, nebulaPart, partition)
      assert(partsForIndex.isEmpty)
    }
  }

}
