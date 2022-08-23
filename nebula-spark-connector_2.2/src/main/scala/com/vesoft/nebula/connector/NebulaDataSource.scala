/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

package com.vesoft.nebula.connector

import com.vesoft.nebula.connector.exception.IllegalOptionException
import com.vesoft.nebula.connector.reader.NebulaRelation
import com.vesoft.nebula.connector.writer.{
  NebulaCommitMessage,
  NebulaEdgeWriter,
  NebulaVertexWriter,
  NebulaWriter,
  NebulaWriterResultRelation
}
import org.apache.spark.TaskContext
import org.apache.spark.sql.{DataFrame, Row, SQLContext, SaveMode}
import org.apache.spark.sql.catalyst.util.CaseInsensitiveMap
import org.apache.spark.sql.sources.{
  BaseRelation,
  CreatableRelationProvider,
  DataSourceRegister,
  RelationProvider
}
import org.slf4j.LoggerFactory

class NebulaDataSource
    extends RelationProvider
    with CreatableRelationProvider
    with DataSourceRegister {
  private val LOG = LoggerFactory.getLogger(this.getClass)

  /**
    * The string that represents the format that nebula data source provider uses.
    */
  override def shortName(): String = "nebula"

  /**
    * Creates a {@link DataSourceReader} to scan the data from Nebula Graph.
    */
  override def createRelation(sqlContext: SQLContext,
                              parameters: Map[String, String]): BaseRelation = {
    val nebulaOptions = getNebulaOptions(parameters, OperaType.READ)

    LOG.info("create relation")
    LOG.info(s"options ${parameters}")

    NebulaRelation(sqlContext, nebulaOptions)
  }

  /**
    * Saves a DataFrame to a destination (using data source-specific parameters)
    */
  override def createRelation(sqlContext: SQLContext,
                              mode: SaveMode,
                              parameters: Map[String, String],
                              data: DataFrame): BaseRelation = {

    val nebulaOptions = getNebulaOptions(parameters, OperaType.WRITE)
    val dataType      = nebulaOptions.dataType
    if (mode == SaveMode.Ignore || mode == SaveMode.ErrorIfExists) {
      LOG.warn(s"Currently do not support mode")
    }

    LOG.info("create writer")
    LOG.info(s"options ${parameters}")

    val schema = data.schema
    val writer: NebulaWriter =
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
        new NebulaVertexWriter(nebulaOptions, vertexIndex, schema).asInstanceOf[NebulaWriter]
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
        new NebulaEdgeWriter(nebulaOptions,
                             edgeFieldsIndex._1,
                             edgeFieldsIndex._2,
                             edgeFieldsIndex._3,
                             schema).asInstanceOf[NebulaWriter]
      }

    val wc: (TaskContext, Iterator[Row]) => NebulaCommitMessage = writer.writeData()
    val rdd                                                     = data.rdd
    val commitMessages                                          = sqlContext.sparkContext.runJob(rdd, wc)

    LOG.info(s"runJob finished...${commitMessages.length}")
    for (msg <- commitMessages) {
      if (msg.executeStatements.nonEmpty) {
        LOG.error(s"failed execs:\n ${msg.executeStatements.toString()}")
      } else {
        LOG.info(s"execs for spark partition ${msg.partitionId} all succeed")
      }
    }
    new NebulaWriterResultRelation(sqlContext, data.schema)
  }

  /**
    * construct nebula options with DataSourceOptions
    */
  def getNebulaOptions(options: Map[String, String],
                       operateType: OperaType.Value): NebulaOptions = {
    val nebulaOptions = new NebulaOptions(CaseInsensitiveMap(options))(operateType)
    nebulaOptions
  }
}
