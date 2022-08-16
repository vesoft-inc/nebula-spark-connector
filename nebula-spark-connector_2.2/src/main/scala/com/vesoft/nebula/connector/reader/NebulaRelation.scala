/* Copyright (c) 2022 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

package com.vesoft.nebula.connector.reader

import com.vesoft.nebula.connector.NebulaOptions
import com.vesoft.nebula.meta.ColumnDef
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.sql.sources.{BaseRelation, TableScan}
import org.apache.spark.sql.types.{DataType, StructType}
import org.slf4j.LoggerFactory

private case class NebulaRelation(override val sqlContext: SQLContext, nebulaOptions: NebulaOptions)
    extends BaseRelation
    with TableScan {
  private val LOG = LoggerFactory.getLogger(this.getClass)

  override val needConversion: Boolean = false

  override def schema: StructType = getSchema(nebulaOptions)

  /**
    * convert [[ColumnDef]] to spark [[DataType]]
    */
  def columnDef2dataType(columnDef: ColumnDef): DataType = {
    null
  }

  /**
    * @todo
    * return the dataset's schema.
    */
  def getSchema(nebulaOptions: NebulaOptions): StructType = {
    null
  }

  /**
    * @todo implement NebulaRDD
    */
  override def buildScan(): RDD[Row] = {
    new NebulaRDD(sqlContext).asInstanceOf[RDD[Row]]
  }
}
