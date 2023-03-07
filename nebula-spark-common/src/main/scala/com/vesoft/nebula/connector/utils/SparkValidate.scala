/* Copyright (c) 2022 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

package com.vesoft.nebula.connector.utils

import org.apache.spark.sql.SparkSession

object SparkValidate {
  def validate(supportedVersions: String*): Unit = {
    val sparkVersion = SparkSession.getActiveSession.map(_.version).getOrElse("UNKNOWN")
    if (sparkVersion != "UNKNOWN" && !supportedVersions.exists(sparkVersion.matches)) {
      throw new RuntimeException(
        s"""Your current spark version ${sparkVersion} is not supported by the current NebulaGraph Spark Connector.
           | please visit https://github.com/vesoft-inc/nebula-spark-connector#version-match to know which Connector you need.
           | """.stripMargin)
    }
  }
}
