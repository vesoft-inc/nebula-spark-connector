/*
 * Copyright (c) 2025 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */
package com.vesoft.nebula.spark.connector.writer

import com.vesoft.nebula.spark.common.exception.IllegalOptionException
import com.vesoft.nebula.spark.common.{DataTypeEnum, NebulaOptions}
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.connector.write.{BatchWrite, SupportsOverwrite, SupportsTruncate, WriteBuilder}
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types.StructType
import org.slf4j.LoggerFactory

class NebulaWriterBuilder(schema: StructType, mode: SaveMode, nebulaOptions: NebulaOptions)
  extends WriteBuilder
    with SupportsOverwrite
    with SupportsTruncate {
  private val LOG = LoggerFactory.getLogger(this.getClass)

  override def buildForBatch(): BatchWrite = {
    val dataType = nebulaOptions.dataType
    if (mode == SaveMode.Ignore || mode == SaveMode.ErrorIfExists) {
      LOG.warn(s"Currently do not support mode")
    }

    if (DataTypeEnum.NODE == DataTypeEnum.withName(dataType)) {
      new NebulaDataSourceNodeWriter(nebulaOptions, schema)
    } else {

      val srcPkFields       = nebulaOptions.srcPkFields
      val dstPkFields       = nebulaOptions.dstPkFields
      val edgePkFieldsIndex = {
        val srcPkIndices = srcPkFields.flatMap { srcPkField =>
          schema.fields.indices.find(i => schema.fields(i).name == srcPkField)
        }
        val dstPkIndices = dstPkFields.flatMap { dstPkField =>
          schema.fields.indices.find(i => schema.fields(i).name == dstPkField)
        }
        if (srcPkIndices.isEmpty || dstPkIndices.isEmpty) {
          throw new IllegalOptionException(
            s"src node primary key fields or dst node primary key fields do not exist in dataframe"
            )
        }
        (srcPkIndices, dstPkIndices)
      }

      new NebulaDataSourceEdgeWriter(nebulaOptions,
                                     edgePkFieldsIndex._1,
                                     edgePkFieldsIndex._2,
                                     schema)
    }
  }

  override def overwrite(filters: Array[Filter]): WriteBuilder = {
    new NebulaWriterBuilder(schema, SaveMode.Overwrite, nebulaOptions)
  }
}
