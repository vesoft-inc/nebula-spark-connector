/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

package com.vesoft.nebula.connector

import com.vesoft.nebula.PropertyType
import com.vesoft.nebula.client.graph.data.{DateTimeWrapper, DurationWrapper, TimeWrapper}
import com.vesoft.nebula.connector.nebula.MetaProvider
import com.vesoft.nebula.meta.{ColumnDef, ColumnTypeDef}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types.{
  BooleanType,
  DataType,
  DataTypes,
  DoubleType,
  FloatType,
  IntegerType,
  LongType,
  StringType,
  StructField,
  StructType,
  TimestampType
}
import org.apache.spark.unsafe.types.UTF8String
import org.slf4j.LoggerFactory

import scala.collection.mutable.ListBuffer

object NebulaUtils {
  private val LOG = LoggerFactory.getLogger(this.getClass)

  /**
    * convert nebula data type to spark sql data type
    */
  def convertDataType(columnTypeDef: ColumnTypeDef): DataType = {

    columnTypeDef.getType match {
      case PropertyType.VID | PropertyType.INT8 | PropertyType.INT16 | PropertyType.INT32 |
          PropertyType.INT64 =>
        LongType
      case PropertyType.BOOL                        => BooleanType
      case PropertyType.FLOAT | PropertyType.DOUBLE => DoubleType
      case PropertyType.TIMESTAMP                   => LongType
      case PropertyType.FIXED_STRING | PropertyType.STRING | PropertyType.DATE | PropertyType.TIME |
          PropertyType.DATETIME | PropertyType.GEOGRAPHY | PropertyType.DURATION =>
        StringType
      case PropertyType.UNKNOWN => throw new IllegalArgumentException("unsupported data type")
    }
  }

  /**
    * get nebula property's SparkSQL data type
    *
    * @param columnDefs column definition
    * @param columnName column name
    *
    * @return {@link DataType}
    */
  def getColDataType(columnDefs: List[ColumnDef], columnName: String): DataType =
    columnDefs
      .collectFirst {
        case columnDef if columnName.equals(new String(columnDef.getName)) =>
          convertDataType(columnDef.getType)
      }
      .getOrElse(throw new IllegalArgumentException(s"column $columnName does not exist in schema"))

  type NebulaValueGetter = (Any, InternalRow, Int) => Unit

  /**
    * make getter
    *
    * @param schema Spark DataFrame schema
    * @return list of NebulaValueGetter
    */
  def makeGetters(schema: StructType): Array[NebulaValueGetter] = {
    schema.fields.map(field => makeGetter(field.dataType))
  }

  private def makeGetter(dataType: DataType): NebulaValueGetter = {
    dataType match {
      case BooleanType =>
        (prop: Any, row: InternalRow, pos: Int) =>
          row.setBoolean(pos, prop.asInstanceOf[Boolean])
      case TimestampType | LongType =>
        (prop: Any, row: InternalRow, pos: Int) =>
          row.setLong(pos, prop.asInstanceOf[Long])
      case FloatType | DoubleType =>
        (prop: Any, row: InternalRow, pos: Int) =>
          row.setDouble(pos, prop.asInstanceOf[Double])
      case IntegerType =>
        (prop: Any, row: InternalRow, pos: Int) =>
          row.setInt(pos, prop.asInstanceOf[Int])
      case _ =>
        (prop: Any, row: InternalRow, pos: Int) =>
          prop match {
            case wrapper: DateTimeWrapper =>
              row.update(pos, UTF8String.fromString(wrapper.getUTCDateTimeStr))
            case wrapper: TimeWrapper =>
              row.update(pos, UTF8String.fromString(wrapper.getUTCTimeStr))
            case wrapper: DurationWrapper =>
              row.update(pos, UTF8String.fromString(wrapper.getDurationString))
            case _ =>
              row.update(pos, UTF8String.fromString(String.valueOf(prop)))
          }
    }
  }

  /**
    * check if a str is numic
    * @param str string
    *
    * @return true if str is numic
    */
  def isNumic(str: String): Boolean =
    str.matches("-?\\d+")

  /**
    * escape the string which contains escape str
    * @param str string
    *
    * @return escaped string
    */
  def escapeUtil(str: String): String =
    str
      .replaceAll("\\\\", "\\\\\\\\")
      .replaceAll("\t", "\\\\t")
      .replaceAll("\n", "\\\\n")
      .replaceAll("\"", "\\\\\"")
      .replaceAll("\'", "\\\\'")
      .replaceAll("\r", "\\\\r")
      .replaceAll("\b", "\\\\b")

  /**
    * return the dataset's schema. Schema includes configured cols in returnCols or includes all properties in nebula.
    */
  def getSchema(nebulaOptions: NebulaOptions): StructType = {
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
    val isVertex = DataTypeEnum.VERTEX.toString.equalsIgnoreCase(nebulaOptions.dataType)

    // construct vertex or edge default prop
    if (isVertex) {
      fields.append(DataTypes.createStructField("_vertexId", DataTypes.StringType, false))
    } else {
      fields.append(DataTypes.createStructField("_srcId", DataTypes.StringType, false))
      fields.append(DataTypes.createStructField("_dstId", DataTypes.StringType, false))
      fields.append(DataTypes.createStructField("_rank", DataTypes.LongType, false))
    }

    // read no column
    if (noColumn) {
      return new StructType(fields.toArray)
    }
    // get tag schema or edge schema
    val schema = if (isVertex) {
      metaProvider.getTag(nebulaOptions.spaceName, nebulaOptions.label)
    } else {
      metaProvider.getEdge(nebulaOptions.spaceName, nebulaOptions.label)
    }

    val schemaCols: Seq[ColumnDef] = schema.columns.asScala

    // read all columns
    if (returnCols.isEmpty) {
      schemaCols.foreach { columnDef =>
        LOG.info(s"prop name ${new String(columnDef.getName)}, type ${columnDef.getType.getType} ")
        fields.append(
          DataTypes.createStructField(new String(columnDef.getName),
                                      convertDataType(columnDef.getType),
                                      true))
      }
    } else {
      for (col: String <- returnCols) {
        fields.append(
          DataTypes
            .createStructField(col, getColDataType(schemaCols.toList, col), true))
      }
    }
    new StructType(fields.toArray)
  }

}
