/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

package com.vesoft.nebula.connector

import com.vesoft.nebula.PropertyType
import com.vesoft.nebula.meta.{ColumnDef, ColumnTypeDef}
import org.apache.spark.sql.types.{
  BooleanType,
  DoubleType,
  LongType,
  StringType,
  StructField,
  StructType
}
import org.scalatest.funsuite.AnyFunSuite

class NebulaUtilsSuite extends AnyFunSuite {

  test("convertDataType") {
    assert(NebulaUtils.convertDataType(new ColumnTypeDef(PropertyType.VID)) == LongType)
    assert(NebulaUtils.convertDataType(new ColumnTypeDef(PropertyType.INT8)) == LongType)
    assert(NebulaUtils.convertDataType(new ColumnTypeDef(PropertyType.INT16)) == LongType)
    assert(NebulaUtils.convertDataType(new ColumnTypeDef(PropertyType.INT32)) == LongType)
    assert(NebulaUtils.convertDataType(new ColumnTypeDef(PropertyType.INT64)) == LongType)
    assert(NebulaUtils.convertDataType(new ColumnTypeDef(PropertyType.TIMESTAMP)) == LongType)
    assert(NebulaUtils.convertDataType(new ColumnTypeDef(PropertyType.BOOL)) == BooleanType)
    assert(NebulaUtils.convertDataType(new ColumnTypeDef(PropertyType.FLOAT)) == DoubleType)
    assert(NebulaUtils.convertDataType(new ColumnTypeDef(PropertyType.DOUBLE)) == DoubleType)
    assert(NebulaUtils.convertDataType(new ColumnTypeDef(PropertyType.FIXED_STRING)) == StringType)
    assert(NebulaUtils.convertDataType(new ColumnTypeDef(PropertyType.STRING)) == StringType)
    assert(NebulaUtils.convertDataType(new ColumnTypeDef(PropertyType.DATE)) == StringType)
    assert(NebulaUtils.convertDataType(new ColumnTypeDef(PropertyType.DATETIME)) == StringType)
    assert(NebulaUtils.convertDataType(new ColumnTypeDef(PropertyType.TIME)) == StringType)
    assert(NebulaUtils.convertDataType(new ColumnTypeDef(PropertyType.GEOGRAPHY)) == StringType)
    assert(NebulaUtils.convertDataType(new ColumnTypeDef(PropertyType.DURATION)) == StringType)
    assertThrows[IllegalArgumentException](
      NebulaUtils.convertDataType(new ColumnTypeDef(PropertyType.UNKNOWN)))
  }

  test("getColDataType") {
    val columnDefs: List[ColumnDef] = List(
      new ColumnDef("col1".getBytes(), new ColumnTypeDef(PropertyType.INT8)),
      new ColumnDef("col2".getBytes(), new ColumnTypeDef(PropertyType.DOUBLE)),
      new ColumnDef("col3".getBytes(), new ColumnTypeDef(PropertyType.STRING)),
      new ColumnDef("col4".getBytes(), new ColumnTypeDef(PropertyType.DATE)),
      new ColumnDef("col5".getBytes(), new ColumnTypeDef(PropertyType.DATETIME)),
      new ColumnDef("col6".getBytes(), new ColumnTypeDef(PropertyType.TIME)),
      new ColumnDef("col7".getBytes(), new ColumnTypeDef(PropertyType.TIMESTAMP)),
      new ColumnDef("col8".getBytes(), new ColumnTypeDef(PropertyType.BOOL))
    )
    assert(NebulaUtils.getColDataType(columnDefs, "col1") == LongType)
    assert(NebulaUtils.getColDataType(columnDefs, "col2") == DoubleType)
    assert(NebulaUtils.getColDataType(columnDefs, "col3") == StringType)
    assert(NebulaUtils.getColDataType(columnDefs, "col4") == StringType)
    assert(NebulaUtils.getColDataType(columnDefs, "col5") == StringType)
    assert(NebulaUtils.getColDataType(columnDefs, "col6") == StringType)
    assert(NebulaUtils.getColDataType(columnDefs, "col7") == LongType)
    assert(NebulaUtils.getColDataType(columnDefs, "col8") == BooleanType)
    assertThrows[IllegalArgumentException](NebulaUtils.getColDataType(columnDefs, "col9"))
  }

  test("makeGetters") {
    val schema = StructType(
      List(
        StructField("col1", LongType, nullable = false),
        StructField("col2", LongType, nullable = true)
      ))
    assert(NebulaUtils.makeGetters(schema).length == 2)
  }

  test("isNumic") {
    assert(NebulaUtils.isNumic("123"))
    assert(NebulaUtils.isNumic("-123"))
    assert(!NebulaUtils.isNumic("1.0"))
    assert(!NebulaUtils.isNumic("a123"))
    assert(!NebulaUtils.isNumic("123b"))
  }

  test("escapeUtil") {
    assert(NebulaUtils.escapeUtil("123").equals("123"))
    // a\bc -> a\\bc
    assert(NebulaUtils.escapeUtil("a\bc").equals("a\\bc"))
    // a\tbc -> a\\tbc
    assert(NebulaUtils.escapeUtil("a\tbc").equals("a\\tbc"))
    // a\nbc -> a\\nbc
    assert(NebulaUtils.escapeUtil("a\nbc").equals("a\\nbc"))
    // a\"bc -> a\\"bc
    assert(NebulaUtils.escapeUtil("a\"bc").equals("a\\\"bc"))
    // a\'bc -> a\\'bc
    assert(NebulaUtils.escapeUtil("a\'bc").equals("a\\'bc"))
    // a\rbc -> a\\rbc
    assert(NebulaUtils.escapeUtil("a\rbc").equals("a\\rbc"))
    // a\bbc -> a\\bbc
    assert(NebulaUtils.escapeUtil("a\bbc").equals("a\\bbc"))
  }
}
