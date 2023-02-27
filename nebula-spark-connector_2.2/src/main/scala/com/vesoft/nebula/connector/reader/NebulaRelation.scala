/* Copyright (c) 2022 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

package com.vesoft.nebula.connector.reader

import com.vesoft.nebula.connector.{NebulaOptions, NebulaUtils}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.sql.sources.{BaseRelation, TableScan}
import org.apache.spark.sql.types.{StructType}
import org.slf4j.LoggerFactory

case class NebulaRelation(override val sqlContext: SQLContext, nebulaOptions: NebulaOptions)
    extends BaseRelation
    with TableScan {
  private val LOG = LoggerFactory.getLogger(this.getClass)

  protected var datasetSchema: StructType = _
  NebulaUtils.getSchema(nebulaOptions)

  override val needConversion: Boolean = false

  override def schema: StructType = {
    if (datasetSchema == null) {
      datasetSchema = NebulaUtils.getSchema(nebulaOptions)
    }
    datasetSchema
  }

  override def buildScan(): RDD[Row] = {

    if (datasetSchema == null) {
      datasetSchema = NebulaUtils.getSchema(nebulaOptions)
    }
    if (nebulaOptions.ngql != null && nebulaOptions.ngql.nonEmpty) {
      new NebulaNgqlRDD(sqlContext, nebulaOptions, datasetSchema).asInstanceOf[RDD[Row]]
    } else {
      new NebulaRDD(sqlContext, nebulaOptions, datasetSchema).asInstanceOf[RDD[Row]]
    }
  }
}
