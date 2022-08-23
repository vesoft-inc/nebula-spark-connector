/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

package com.vesoft.nebula.connector.writer

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.sources.BaseRelation
import org.apache.spark.sql.types.StructType

class NebulaWriterResultRelation(SQLContext: SQLContext, userDefSchema: StructType)
    extends BaseRelation {
  override def sqlContext: SQLContext = SQLContext

  override def schema: StructType = userDefSchema
}
