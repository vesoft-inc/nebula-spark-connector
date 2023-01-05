/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

package com.vesoft.nebula.connector

import java.util
import java.util.Map.Entry

import com.vesoft.nebula.connector.nebula.MetaProvider
import com.vesoft.nebula.connector.reader.SimpleScanBuilder
import com.vesoft.nebula.connector.utils.Validations
import com.vesoft.nebula.meta.ColumnDef
import org.apache.spark.sql.catalyst.util.CaseInsensitiveMap
import org.apache.spark.sql.connector.catalog.{
  SupportsRead,
  SupportsWrite,
  Table,
  TableCapability,
  TableProvider
}
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.connector.read.ScanBuilder
import org.apache.spark.sql.connector.write.{LogicalWriteInfo, WriteBuilder}
import org.apache.spark.sql.sources.DataSourceRegister
import org.apache.spark.sql.types.{DataTypes, StructField, StructType}
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.slf4j.LoggerFactory

import scala.collection.mutable.ListBuffer
import scala.jdk.CollectionConverters.asScalaSetConverter

class NebulaDataSource extends TableProvider with DataSourceRegister {
  private val LOG = LoggerFactory.getLogger(this.getClass)

  Validations.validateSparkVersion("3.*")

  private var schema: StructType           = null
  private var nebulaOptions: NebulaOptions = _

  /**
    * The string that represents the format that nebula data source provider uses.
    */
  override def shortName(): String = "nebula"

  override def supportsExternalMetadata(): Boolean = true

  override def inferSchema(caseInsensitiveStringMap: CaseInsensitiveStringMap): StructType = {
    if (schema == null) {
      nebulaOptions = getNebulaOptions(caseInsensitiveStringMap)
      if (nebulaOptions.operaType == OperaType.READ) {
        schema = NebulaUtils.getSchema(nebulaOptions)
      } else {
        schema = new StructType()
      }
    }
    schema
  }

  override def getTable(tableSchema: StructType,
                        transforms: Array[Transform],
                        map: util.Map[String, String]): Table = {
    if (nebulaOptions == null) {
      nebulaOptions = getNebulaOptions(new CaseInsensitiveStringMap(map))
    }
    new NebulaTable(tableSchema, nebulaOptions)
  }

  /**
    * construct nebula options with DataSourceOptions
    */
  private def getNebulaOptions(
      caseInsensitiveStringMap: CaseInsensitiveStringMap): NebulaOptions = {
    var parameters: Map[String, String] = Map()
    for (entry: Entry[String, String] <- caseInsensitiveStringMap
           .asCaseSensitiveMap()
           .entrySet()
           .asScala) {
      parameters += (entry.getKey -> entry.getValue)
    }
    val nebulaOptions = new NebulaOptions(CaseInsensitiveMap(parameters))
    nebulaOptions
  }

}
