/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
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
    val partsForIndex0  = PartitionUtils.getScanParts(1, nebulaPart, partition)
    assert(partsForIndex0.size == 1)
    assert(partsForIndex0.head == 1)

    val partsForIndex1 = PartitionUtils.getScanParts(2, nebulaPart, partition)
    assert(partsForIndex1.head == 2)

    val partsForIndex2 = PartitionUtils.getScanParts(3, nebulaPart, partition)
    assert(partsForIndex2.head == 3)

    val partsForIndex3 = PartitionUtils.getScanParts(4, nebulaPart, partition)
    assert(partsForIndex3.head == 4)

    val partsForIndex4 = PartitionUtils.getScanParts(5, nebulaPart, partition)
    assert(partsForIndex4.head == 5)

    val partsForIndex5 = PartitionUtils.getScanParts(6, nebulaPart, partition)
    assert(partsForIndex5.head == 6)

    val partsForIndex6 = PartitionUtils.getScanParts(7, nebulaPart, partition)
    assert(partsForIndex6.head == 7)

    val partsForIndex7 = PartitionUtils.getScanParts(8, nebulaPart, partition)
    assert(partsForIndex7.head == 8)

    val partsForIndex8 = PartitionUtils.getScanParts(9, nebulaPart, partition)
    assert(partsForIndex8.head == 9)

    val partsForIndex9 = PartitionUtils.getScanParts(10, nebulaPart, partition)
    assert(partsForIndex9.head == 10)
  }

  test("getScanParts: nebula part is more than spark partition") {
    val nebulaPart: Int = 20
    val partsForIndex0  = PartitionUtils.getScanParts(1, nebulaPart, partition)
    assert(partsForIndex0.contains(1) && partsForIndex0.contains(11))

    val partsForIndex1 = PartitionUtils.getScanParts(2, nebulaPart, partition)
    assert(partsForIndex1.contains(2) && partsForIndex1.contains(12))

    val partsForIndex2 = PartitionUtils.getScanParts(3, nebulaPart, partition)
    assert(partsForIndex2.contains(3) && partsForIndex2.contains(13))

    val partsForIndex3 = PartitionUtils.getScanParts(4, nebulaPart, partition)
    assert(partsForIndex3.contains(4) && partsForIndex3.contains(14))

    val partsForIndex4 = PartitionUtils.getScanParts(5, nebulaPart, partition)
    assert(partsForIndex4.contains(5) && partsForIndex4.contains(15))

    val partsForIndex5 = PartitionUtils.getScanParts(6, nebulaPart, partition)
    assert(partsForIndex5.contains(6) && partsForIndex5.contains(16))

    val partsForIndex6 = PartitionUtils.getScanParts(7, nebulaPart, partition)
    assert(partsForIndex6.contains(7) && partsForIndex6.contains(17))

    val partsForIndex7 = PartitionUtils.getScanParts(8, nebulaPart, partition)
    assert(partsForIndex7.contains(8) && partsForIndex7.contains(18))

    val partsForIndex8 = PartitionUtils.getScanParts(9, nebulaPart, partition)
    assert(partsForIndex8.contains(9) && partsForIndex8.contains(19))

    val partsForIndex9 = PartitionUtils.getScanParts(10, nebulaPart, partition)
    assert(partsForIndex9.contains(10) && partsForIndex9.contains(20))
  }

  test("getScanParts: nebula part is less than spark partition") {
    val nebulaPart: Int = 5
    val partsForIndex0  = PartitionUtils.getScanParts(1, nebulaPart, partition)
    assert(partsForIndex0.contains(1))

    val partsForIndex1 = PartitionUtils.getScanParts(2, nebulaPart, partition)
    assert(partsForIndex1.contains(2))

    val partsForIndex2 = PartitionUtils.getScanParts(3, nebulaPart, partition)
    assert(partsForIndex2.contains(3))

    val partsForIndex3 = PartitionUtils.getScanParts(4, nebulaPart, partition)
    assert(partsForIndex3.contains(4))

    val partsForIndex4 = PartitionUtils.getScanParts(5, nebulaPart, partition)
    assert(partsForIndex4.contains(5))

    val partsForIndex5 = PartitionUtils.getScanParts(6, nebulaPart, partition)
    assert(partsForIndex5.isEmpty)

    val partsForIndex6 = PartitionUtils.getScanParts(7, nebulaPart, partition)
    assert(partsForIndex6.isEmpty)

    val partsForIndex7 = PartitionUtils.getScanParts(8, nebulaPart, partition)
    assert(partsForIndex7.isEmpty)

    val partsForIndex8 = PartitionUtils.getScanParts(9, nebulaPart, partition)
    assert(partsForIndex8.isEmpty)

    val partsForIndex9 = PartitionUtils.getScanParts(10, nebulaPart, partition)
    assert(partsForIndex9.isEmpty)
  }

}
