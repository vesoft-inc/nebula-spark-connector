/* Copyright (c) 2022 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

package com.vesoft.nebula.connector.writer

import com.vesoft.nebula.connector.exception.IllegalOptionException
import com.vesoft.nebula.connector.{DataTypeEnum, NebulaOptions}
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.connector.write.{
  BatchWrite,
  SupportsOverwrite,
  SupportsTruncate,
  WriteBuilder
}
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types.StructType

class NebulaWriterBuilder(schema: StructType, saveMode: SaveMode, nebulaOptions: NebulaOptions)
    extends WriteBuilder
    with SupportsOverwrite
    with SupportsTruncate {

  override def buildForBatch(): BatchWrite = {
    val dataType = nebulaOptions.dataType
    if (DataTypeEnum.VERTEX == DataTypeEnum.withName(dataType)) {
      val vertexFiled = nebulaOptions.vertexField
      val vertexIndex: Int = {
        var index: Int = -1
        for (i <- schema.fields.indices) {
          if (schema.fields(i).name.equals(vertexFiled)) {
            index = i
          }
        }
        if (index < 0) {
          throw new IllegalOptionException(
            s" vertex field ${vertexFiled} does not exist in dataframe")
        }
        index
      }
      new NebulaDataSourceVertexWriter(nebulaOptions, vertexIndex, schema)
    } else {
      val srcVertexFiled = nebulaOptions.srcVertexField
      val dstVertexField = nebulaOptions.dstVertexField
      val rankExist      = !nebulaOptions.rankField.isEmpty
      val edgeFieldsIndex = {
        var srcIndex: Int  = -1
        var dstIndex: Int  = -1
        var rankIndex: Int = -1
        for (i <- schema.fields.indices) {
          if (schema.fields(i).name.equals(srcVertexFiled)) {
            srcIndex = i
          }
          if (schema.fields(i).name.equals(dstVertexField)) {
            dstIndex = i
          }
          if (rankExist) {
            if (schema.fields(i).name.equals(nebulaOptions.rankField)) {
              rankIndex = i
            }
          }
        }
        // check src filed and dst field
        if (srcIndex < 0 || dstIndex < 0) {
          throw new IllegalOptionException(
            s" srcVertex field ${srcVertexFiled} or dstVertex field ${dstVertexField} do not exist in dataframe")
        }
        // check rank field
        if (rankExist && rankIndex < 0) {
          throw new IllegalOptionException(s"rank field does not exist in dataframe")
        }

        if (!rankExist) {
          (srcIndex, dstIndex, Option.empty)
        } else {
          (srcIndex, dstIndex, Option(rankIndex))
        }

      }
      new NebulaDataSourceEdgeWriter(nebulaOptions,
                                     edgeFieldsIndex._1,
                                     edgeFieldsIndex._2,
                                     edgeFieldsIndex._3,
                                     schema)
    }
  }

  override def overwrite(filters: Array[Filter]): WriteBuilder = {
    new NebulaWriterBuilder(schema, SaveMode.Overwrite, nebulaOptions)
  }
}
