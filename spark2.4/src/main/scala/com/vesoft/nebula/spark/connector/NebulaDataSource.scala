/*
 * Copyright (c) 2025 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

package com.vesoft.nebula.spark.connector

import com.vesoft.nebula.spark.connector.reader.{NebulaDataSourceEdgeReader, NebulaDataSourceGqlReader, NebulaDataSourceNodeReader}
import com.vesoft.nebula.spark.common.{DataTypeEnum, NebulaOptions}
import com.vesoft.nebula.spark.common.exception.IllegalOptionException

import java.util.Map.Entry
import java.util.Optional
import com.vesoft.nebula.spark.connector.writer.{NebulaDataSourceEdgeWriter, NebulaDataSourceNodeWriter}
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.catalyst.util.CaseInsensitiveMap
import org.apache.spark.sql.sources.DataSourceRegister
import org.apache.spark.sql.sources.v2.reader.DataSourceReader
import org.apache.spark.sql.sources.v2.writer.DataSourceWriter
import org.apache.spark.sql.sources.v2.{DataSourceOptions, DataSourceV2, ReadSupport, WriteSupport}
import org.apache.spark.sql.types.StructType
import org.slf4j.LoggerFactory

import scala.collection.JavaConversions.iterableAsScalaIterable
import scala.collection.mutable.ListBuffer

class NebulaDataSource
  extends DataSourceV2
    with ReadSupport
    with WriteSupport
    with DataSourceRegister {
  private val LOG = LoggerFactory.getLogger(this.getClass)

  /**
   * The string that represents the format that nebula data source provider uses.
   */
  override def shortName(): String = "nebula"

  /**
   * Creates a {@link DataSourceReader} to scan the data from Nebula Graph.
   */
  override def createReader(options: DataSourceOptions): DataSourceReader = {
    val nebulaOptions = getNebulaOptions(options)
    val dataType      = nebulaOptions.dataType
    LOG.info("create NebulaGraph reader")
    val parameters = options.asMap()
    parameters.remove(NebulaOptions.PASSWD)
    parameters.remove(NebulaOptions.AUTHOPTIONS)
    LOG.info(s"options: ${parameters}")

    if (DataTypeEnum.NODE == DataTypeEnum.withName(dataType)) {
      new NebulaDataSourceNodeReader(nebulaOptions)
    } else if (DataTypeEnum.EDGE == DataTypeEnum.withName(dataType)) {
      new NebulaDataSourceEdgeReader(nebulaOptions)
    } else {
      new NebulaDataSourceGqlReader(nebulaOptions)
    }
  }

  /**
   * Creates an optional {@link DataSourceWriter} to save the data to Nebula Graph.
   */
  override def createWriter(writeUUID: String,
                            schema: StructType,
                            mode: SaveMode,
                            options: DataSourceOptions): Optional[DataSourceWriter] = {

    val nebulaOptions = getNebulaOptions(options)
    val dataType      = nebulaOptions.dataType
    if (mode == SaveMode.Ignore || mode == SaveMode.ErrorIfExists) {
      LOG.warn(s"Currently do not support mode")
    }

    LOG.info("create writer")
    val parameters = options.asMap()
    parameters.remove(NebulaOptions.PASSWD)
    parameters.remove(NebulaOptions.AUTHOPTIONS)
    LOG.info(s"options ${parameters}")

    if (DataTypeEnum.NODE == DataTypeEnum.withName(dataType)) {
      Optional.of(new NebulaDataSourceNodeWriter(nebulaOptions, schema))
    } else {
      val srcPkFields     = nebulaOptions.srcPkFields
      val dstPkFields     = nebulaOptions.dstPkFields
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

      Optional.of(
        new NebulaDataSourceEdgeWriter(nebulaOptions,
                                       edgePkFieldsIndex._1,
                                       edgePkFieldsIndex._2,
                                       schema))
    }
  }

  /**
   * construct nebula options with DataSourceOptions
   */
  def getNebulaOptions(options: DataSourceOptions): NebulaOptions = {
    var parameters: Map[String, String] = Map()
    for (entry: Entry[String, String] <- options.asMap().entrySet) {
      parameters += (entry.getKey -> entry.getValue)
    }
    val nebulaOptions = new NebulaOptions(CaseInsensitiveMap(parameters))
    nebulaOptions
  }
}
