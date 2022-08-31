/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

package com.vesoft.nebula.connector

import com.vesoft.nebula.PropertyType
import com.vesoft.nebula.client.graph.data.{DateTimeWrapper, DurationWrapper, TimeWrapper}
import com.vesoft.nebula.meta.{ColumnDef, ColumnTypeDef}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types.{
  BooleanType,
  DataType,
  DoubleType,
  FloatType,
  IntegerType,
  LongType,
  StringType,
  StructType,
  TimestampType
}
import org.apache.spark.unsafe.types.UTF8String
import org.slf4j.LoggerFactory

object NebulaUtils {
  private val LOG = LoggerFactory.getLogger(this.getClass)

  var nebulaOptions: NebulaOptions    = _
  var parameters: Map[String, String] = Map()

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
  def getColDataType(columnDefs: List[ColumnDef], columnName: String): DataType = {
    for (columnDef <- columnDefs) {
      if (columnName.equals(new String(columnDef.getName))) {
        return convertDataType(columnDef.getType)
      }
    }
    throw new IllegalArgumentException(s"column $columnName does not exist in schema")
  }

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
          if (prop.isInstanceOf[DateTimeWrapper]) {
            row.update(pos,
                       UTF8String.fromString(prop.asInstanceOf[DateTimeWrapper].getUTCDateTimeStr))
          } else if (prop.isInstanceOf[TimeWrapper]) {
            row.update(pos, UTF8String.fromString(prop.asInstanceOf[TimeWrapper].getUTCTimeStr))
          } else if (prop.isInstanceOf[DurationWrapper]) {
            row.update(pos,
                       UTF8String.fromString(prop.asInstanceOf[DurationWrapper].getDurationString))
          } else {
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
  def isNumic(str: String): Boolean = {
    val newStr: String = if (str.startsWith("-")) {
      str.substring(1)
    } else { str }

    for (char <- newStr.toCharArray) {
      if (!Character.isDigit(char)) return false
    }
    true
  }

  /**
    * escape the string which contains escape str
    * @param str string
    *
    * @return escaped string
    */
  def escapeUtil(str: String): String = {
    var s = str
    if (s.contains("\\")) {
      s = s.replaceAll("\\\\", "\\\\\\\\")
    }
    if (s.contains("\t")) {
      s = s.replaceAll("\t", "\\\\t")
    }
    if (s.contains("\n")) {
      s = s.replaceAll("\n", "\\\\n")
    }
    if (s.contains("\"")) {
      s = s.replaceAll("\"", "\\\\\"")
    }
    if (s.contains("\'")) {
      s = s.replaceAll("\'", "\\\\'")
    }
    if (s.contains("\r")) {
      s = s.replaceAll("\r", "\\\\r")
    }
    if (s.contains("\b")) {
      s = s.replaceAll("\b", "\\\\b")
    }
    s
  }

}
