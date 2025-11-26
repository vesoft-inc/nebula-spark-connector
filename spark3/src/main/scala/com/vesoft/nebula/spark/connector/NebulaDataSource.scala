/*
 * Copyright (c) 2025 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */
package com.vesoft.nebula.spark.connector

import com.vesoft.nebula.spark.common.{DataTypeEnum, NebulaOptions, NebulaUtils, OperaType}
import org.apache.spark.sql.catalyst.util.CaseInsensitiveMap
import org.apache.spark.sql.connector.catalog.{Table, TableProvider}
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.sources.DataSourceRegister
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.slf4j.LoggerFactory

import java.util.Map.Entry
import scala.jdk.CollectionConverters.asScalaSetConverter

class NebulaDataSource extends TableProvider with DataSourceRegister {
  private val LOG = LoggerFactory.getLogger(this.getClass)

  private var schema       : StructType    = null
  private var nebulaOptions: NebulaOptions = _


  /**
   * The string that represents the format that nebula data source provider uses.
   */
  override def shortName(): String = "nebula"


  override def inferSchema(caseInsensitiveStringMap: CaseInsensitiveStringMap): StructType = {
    if (schema == null) {
      nebulaOptions = getNebulaOptions(caseInsensitiveStringMap)
      if (nebulaOptions.operaType == OperaType.READ) {
        if (DataTypeEnum.GQL == DataTypeEnum.withName(nebulaOptions.dataType)) {
          schema = NebulaUtils.getSchemaForGql(nebulaOptions)
        } else {
          schema = NebulaUtils.getSchema(nebulaOptions)
        }
      } else {
        schema = new StructType()
      }
    }
    schema
  }

  override def getTable(tableSchema: StructType,
                        transforms: Array[Transform],
                        map: java.util.Map[String, String]): Table = {
    if (nebulaOptions == null) {
      nebulaOptions = getNebulaOptions(new CaseInsensitiveStringMap(map))
    }
    new NebulaTable(tableSchema, nebulaOptions)
  }


  /**
   * construct nebula options with DataSourceOptions
   */
  private def getNebulaOptions(caseInsensitiveStringMap: CaseInsensitiveStringMap): NebulaOptions = {
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
