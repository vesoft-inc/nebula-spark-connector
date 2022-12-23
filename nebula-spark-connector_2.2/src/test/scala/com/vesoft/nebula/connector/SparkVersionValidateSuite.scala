/* Copyright (c) 2022 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

package com.vesoft.nebula.connector

import com.vesoft.nebula.connector.utils.SparkValidate
import org.apache.spark.sql.SparkSession
import org.scalatest.funsuite.AnyFunSuite

class SparkVersionValidateSuite {
  class SparkVersionValidateSuite extends AnyFunSuite {
    test("spark version validate") {
      try {
        val version = SparkSession.getActiveSession.map(_.version).getOrElse("UNKNOWN")
        SparkValidate.validate("2.2.*")
      } catch {
        case e: Exception => assert(false)
      }
    }
  }
}
