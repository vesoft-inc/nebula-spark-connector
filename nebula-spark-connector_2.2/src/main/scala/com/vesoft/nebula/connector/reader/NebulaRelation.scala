/* Copyright (c) 2022 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

package com.vesoft.nebula.connector.reader

import com.vesoft.nebula.connector.nebula.MetaProvider
import com.vesoft.nebula.connector.{DataTypeEnum, NebulaOptions, NebulaUtils}
import com.vesoft.nebula.meta.ColumnDef
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.sql.sources.{BaseRelation, TableScan}
import org.apache.spark.sql.types.{DataType, DataTypes, StructField, StructType}
import org.slf4j.LoggerFactory

import scala.collection.mutable.ListBuffer

case class NebulaRelation(override val sqlContext: SQLContext, nebulaOptions: NebulaOptions)
    extends BaseRelation
    with TableScan {
  private val LOG = LoggerFactory.getLogger(this.getClass)

  protected lazy val datasetSchema: StructType = getSchema(nebulaOptions)

  override val needConversion: Boolean = false

  override def schema: StructType = getSchema(nebulaOptions)

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

  override def buildScan(): RDD[Row] = {
    if (nebulaOptions.ngql != null && nebulaOptions.ngql.nonEmpty) {
      new NebulaNgqlRDD(sqlContext, nebulaOptions, datasetSchema).asInstanceOf[RDD[Row]]
    } else {
      new NebulaRDD(sqlContext, nebulaOptions, datasetSchema).asInstanceOf[RDD[Row]]
    }
  }
}
