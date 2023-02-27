/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

package com.vesoft.nebula.connector.utils

import org.apache.spark.sql.SparkSession

object Validations {
  def validateSparkVersion(supportedVersions: String*): Unit = {
    val sparkVersion = SparkSession.getActiveSession.map { _.version }.getOrElse("UNKNOWN")
    if (!(sparkVersion == "UNKNOWN" || supportedVersions.exists(sparkVersion.matches))) {
      throw new RuntimeException(
        s"Your current spark version ${sparkVersion} is not supported bt the current connector.")
    }
  }
}
