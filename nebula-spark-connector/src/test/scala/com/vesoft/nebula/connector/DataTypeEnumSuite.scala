/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

package com.vesoft.nebula.connector

import org.scalatest.funsuite.AnyFunSuite

class DataTypeEnumSuite extends AnyFunSuite {

  test("validDataType") {
    assert(DataTypeEnum.validDataType("vertex"))
    assert(DataTypeEnum.validDataType("VERTEX"))
    assert(DataTypeEnum.validDataType("edge"))
    assert(DataTypeEnum.validDataType("EDGE"))
    assert(!DataTypeEnum.validDataType("relation"))
  }

}
