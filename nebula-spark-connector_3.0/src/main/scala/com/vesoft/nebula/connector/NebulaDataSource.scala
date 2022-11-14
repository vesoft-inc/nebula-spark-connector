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
        schema = getSchema(nebulaOptions)
      } else {
        schema = new StructType()
      }
    }
    schema
  }

  override def getTable(tableSchema: StructType,
                        transforms: Array[Transform],
                        map: util.Map[String, String]): Table = {
    new NebulaTable(tableSchema, nebulaOptions)
  }

  /**
    * return the dataset's schema. Schema includes configured cols in returnCols or includes all properties in nebula.
    */
  private def getSchema(nebulaOptions: NebulaOptions): StructType = {
    val returnCols                      = nebulaOptions.getReturnCols
    val noColumn                        = nebulaOptions.noColumn
    val fields: ListBuffer[StructField] = new ListBuffer[StructField]
    val metaProvider = new MetaProvider(
      nebulaOptions.getMetaAddress,
      nebulaOptions.timeout,
      nebulaOptions.connectionRetry,
      nebulaOptions.executionRetry,
      nebulaOptions.enableMetaSSL,
      nebulaOptions.sslSignType,
      nebulaOptions.caSignParam,
      nebulaOptions.selfSignParam
    )

    import scala.collection.JavaConverters._
    var schemaCols: Seq[ColumnDef] = Seq()
    val isVertex                   = DataTypeEnum.VERTEX.toString.equalsIgnoreCase(nebulaOptions.dataType)

    // construct vertex or edge default prop
    if (isVertex) {
      fields.append(DataTypes.createStructField("_vertexId", DataTypes.StringType, false))
    } else {
      fields.append(DataTypes.createStructField("_srcId", DataTypes.StringType, false))
      fields.append(DataTypes.createStructField("_dstId", DataTypes.StringType, false))
      fields.append(DataTypes.createStructField("_rank", DataTypes.LongType, false))
    }

    var dataSchema: StructType = null
    // read no column
    if (noColumn) {
      dataSchema = new StructType(fields.toArray)
      return dataSchema
    }
    // get tag schema or edge schema
    val schema = if (isVertex) {
      metaProvider.getTag(nebulaOptions.spaceName, nebulaOptions.label)
    } else {
      metaProvider.getEdge(nebulaOptions.spaceName, nebulaOptions.label)
    }

    schemaCols = schema.columns.asScala

    // read all columns
    if (returnCols.isEmpty) {
      schemaCols.foreach(columnDef => {
        LOG.info(s"prop name ${new String(columnDef.getName)}, type ${columnDef.getType.getType} ")
        fields.append(
          DataTypes.createStructField(new String(columnDef.getName),
                                      NebulaUtils.convertDataType(columnDef.getType),
                                      true))
      })
    } else {
      for (col: String <- returnCols) {
        fields.append(
          DataTypes
            .createStructField(col, NebulaUtils.getColDataType(schemaCols.toList, col), true))
      }
    }
    dataSchema = new StructType(fields.toArray)
    dataSchema
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
