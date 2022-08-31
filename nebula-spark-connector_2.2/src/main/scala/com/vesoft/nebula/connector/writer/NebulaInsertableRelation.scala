/* Copyright (c) 2022 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

package com.vesoft.nebula.connector.writer

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.sources.InsertableRelation

class NebulaInsertableRelation extends InsertableRelation {
  override def insert(data: DataFrame, overwrite: Boolean): Unit = {}
}
