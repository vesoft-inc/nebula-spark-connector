/*
 * Copyright (c) 2025 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */
package com.vesoft.nebula.spark.connector

import com.vesoft.nebula.spark.connector.reader.SimpleScanBuilder
import com.vesoft.nebula.spark.connector.writer.NebulaWriterBuilder
import com.vesoft.nebula.spark.common.{DataTypeEnum, NebulaOptions}
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.connector.catalog.{SupportsRead, SupportsWrite, Table, TableCapability}
import org.apache.spark.sql.connector.read.ScanBuilder
import org.apache.spark.sql.connector.write.{LogicalWriteInfo, WriteBuilder}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.slf4j.LoggerFactory

import java.util
import scala.collection.mutable
import scala.jdk.CollectionConverters.{mapAsScalaMapConverter, setAsJavaSetConverter}

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
    val options = caseInsensitiveStringMap
      .asCaseSensitiveMap()
      .asScala
      .filterNot { case (k, _) => k == NebulaOptions.PASSWD || k == NebulaOptions.AUTHOPTIONS }.toMap
    LOG.info(s"options: ${options}")

    new SimpleScanBuilder(nebulaOptions)
  }


  /**
   * Creates an optional {@link DataSourceWriter} to save the data to Nebula Graph.
   */
  override def newWriteBuilder(logicalWriteInfo: LogicalWriteInfo): WriteBuilder = {
    LOG.info("create writer")
    val options = logicalWriteInfo
      .options()
      .asCaseSensitiveMap()
      .asScala
      .filterNot { case (k, _) => k == NebulaOptions.PASSWD || k == NebulaOptions.AUTHOPTIONS }
      .toMap
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
