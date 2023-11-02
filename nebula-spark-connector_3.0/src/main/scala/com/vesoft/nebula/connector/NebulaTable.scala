/* Copyright (c) 2022 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

package com.vesoft.nebula.connector

import java.util
import java.util.Map.Entry
import com.vesoft.nebula.connector.reader.SimpleScanBuilder
import com.vesoft.nebula.connector.writer.NebulaWriterBuilder
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.catalyst.util.CaseInsensitiveMap
import org.apache.spark.sql.connector.catalog.{SupportsRead, SupportsWrite, Table, TableCapability}
import org.apache.spark.sql.connector.read.ScanBuilder
import org.apache.spark.sql.connector.write.{LogicalWriteInfo, WriteBuilder}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.slf4j.LoggerFactory

import scala.collection.JavaConverters._
import scala.collection.mutable

class NebulaTable(schema: StructType, nebulaOptions: NebulaOptions)
    extends Table
    with SupportsRead
    with SupportsWrite {

  private val LOG = LoggerFactory.getLogger(this.getClass)

  /**
    * Creates a {@link DataSourceReader} to scan the data from Nebula Graph.
    */
  override def newScanBuilder(caseInsensitiveStringMap: CaseInsensitiveStringMap): ScanBuilder = {
    LOG.info("create scan builder")
    val options    = new mutable.HashMap[String, String]()
    val parameters = caseInsensitiveStringMap.asCaseSensitiveMap().asScala
    for (k: String <- parameters.keySet) {
      if (!k.equalsIgnoreCase("passwd")) {
        options += (k -> parameters(k))
      }
    }
    LOG.info(s"options ${options}")

    new SimpleScanBuilder(nebulaOptions, schema)
  }

  /**
    * Creates an optional {@link DataSourceWriter} to save the data to Nebula Graph.
    */
  override def newWriteBuilder(logicalWriteInfo: LogicalWriteInfo): WriteBuilder = {
    LOG.info("create writer")
    val options    = new mutable.HashMap[String, String]()
    val parameters = logicalWriteInfo.options().asCaseSensitiveMap().asScala
    for (k: String <- parameters.keySet) {
      if (!k.equalsIgnoreCase("passwd")) {
        options += (k -> parameters(k))
      }
    }
    LOG.info(s"options ${options}")
    new NebulaWriterBuilder(logicalWriteInfo.schema(), SaveMode.Append, nebulaOptions)
  }

  /**
    * NebulaGraph table name
    */
  override def name(): String = {
    nebulaOptions.label
  }

  override def schema(): StructType = schema

  override def capabilities(): util.Set[TableCapability] =
    Set(
      TableCapability.BATCH_READ,
      TableCapability.BATCH_WRITE,
      TableCapability.ACCEPT_ANY_SCHEMA,
      TableCapability.OVERWRITE_BY_FILTER,
      TableCapability.OVERWRITE_DYNAMIC,
      TableCapability.STREAMING_WRITE,
      TableCapability.MICRO_BATCH_READ
    ).asJava

}
