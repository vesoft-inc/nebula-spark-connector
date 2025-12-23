/*
 * Copyright (c) 2025 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */
package com.vesoft.nebula.spark

import com.vesoft.nebula.spark.common.utils.SparkValidate
import com.vesoft.nebula.spark.common.{DataTypeEnum, GqlNebulaConfig, NebulaConnectionConfig, NebulaOptions, OperaType, ReadNebulaConfig, WriteNebulaConfig, WriteNebulaEdgeConfig, WriteNebulaNodeConfig}
import org.apache.spark.sql.{DataFrame, DataFrameReader, DataFrameWriter, Encoder, Encoders, Row, SaveMode}

package object connector {

  /**
   * spark writer for nebula graph
   */
  implicit class NebulaDataFrameWriter(writer: DataFrameWriter[Row]) {

    var connectionConfig : NebulaConnectionConfig = _
    var writeNebulaConfig: WriteNebulaConfig      = _

    /**
     * config nebula connection
     *
     * @param connectionConfig  connection parameters
     * @param writeNebulaConfig write parameters for vertex or edge
     */
    def nebula(connectionConfig: NebulaConnectionConfig,
               writeNebulaConfig: WriteNebulaConfig): NebulaDataFrameWriter = {
      SparkValidate.validate("3.*.*")
      this.connectionConfig = connectionConfig
      this.writeNebulaConfig = writeNebulaConfig
      this
    }

    /**
     * write dataframe into nebula vertex
     */
    @Deprecated
    def writeVertices(): Unit = writeNodes()

    def writeNodes(): Unit = {
      assert(connectionConfig != null && writeNebulaConfig != null,
             "nebula config is not set, please call nebula() before writeVertices")
      val writeConfig = writeNebulaConfig.asInstanceOf[WriteNebulaNodeConfig]
      val dfWriter    = writer
        .format(classOf[NebulaDataSource].getName)
        .mode(SaveMode.Overwrite)
        .option(NebulaOptions.TYPE, DataTypeEnum.NODE.toString)
        .option(NebulaOptions.OPERATE_TYPE, OperaType.WRITE.toString)
        .option(NebulaOptions.USER_NAME, connectionConfig.getUser)
        .option(NebulaOptions.AUTHOPTIONS, connectionConfig.getAuthOptions)
        .option(NebulaOptions.GRAPH_ADDRESS, connectionConfig.getGraphAddress)
        .option(NebulaOptions.TIMEOUT, connectionConfig.getTimeout)
        .option(NebulaOptions.EXECUTION_RETRY, connectionConfig.getExecRetry)
        .option(NebulaOptions.EXECUTION_RETRY_INTERVAL, connectionConfig.getExecRetryIntervalMs)
        .option(NebulaOptions.GRAPH_NAME, writeConfig.getGraphName)
        .option(NebulaOptions.LABEL, writeConfig.getNodeType)
        .option(NebulaOptions.BATCH_SIZE, writeConfig.getBatchSize)
        .option(NebulaOptions.WRITE_MODE, writeConfig.getWriteMode)
        .option(NebulaOptions.DISABLE_WRITE_LOG, writeConfig.isDisableWriteLog)
        .option(NebulaOptions.ERROR_WHEN_FAILED, writeConfig.throwErrorWhenFailed)
        .option(NebulaOptions.ENABLE_TLS, connectionConfig.getEnableTls)
        .option(NebulaOptions.DISABLE_VERIFY_SERVER_CERT, connectionConfig.getDisableVerifyServerCert)
        .option(NebulaOptions.TLS_CA, connectionConfig.getTlsCaPath)
        .option(NebulaOptions.TLS_CERT, connectionConfig.getTlsCertPath)
        .option(NebulaOptions.TLS_KEY, connectionConfig.getTlsKeyPath)

      if (writeConfig.getSchema != null) {
        dfWriter.option(NebulaOptions.SCHEMA, writeConfig.getSchema)
      }
      if (writeConfig.getZonedDateTimeFormat != null) {
        dfWriter.option(NebulaOptions.ZONED_DATETIME_FORMAT, writeConfig.getZonedDateTimeFormat)
      }
      if (writeConfig.getLocalDateTimeFormat != null) {
        dfWriter.option(NebulaOptions.LOCAL_DATETIME_FORMAT, writeConfig.getLocalDateTimeFormat)
      }
      if (writeConfig.getZonedTimeFormat != null) {
        dfWriter.option(NebulaOptions.ZONED_TIME_FORMAT, writeConfig.getZonedTimeFormat)
      }
      if (writeConfig.getLocalTimeFormat != null) {
        dfWriter.option(NebulaOptions.LOCAL_TIME_FORMAT, writeConfig.getLocalTimeFormat)
      }
      dfWriter.save()
    }

    /**
     * write dataframe into nebula edge type
     */
    def writeEdges(): Unit = {

      assert(connectionConfig != null && writeNebulaConfig != null,
             "nebula config is not set, please call nebula() before writeEdges")
      val writeConfig = writeNebulaConfig.asInstanceOf[WriteNebulaEdgeConfig]
      val dfWriter    = writer
        .format(classOf[NebulaDataSource].getName)
        .mode(SaveMode.Overwrite)
        .option(NebulaOptions.TYPE, DataTypeEnum.EDGE.toString)
        .option(NebulaOptions.OPERATE_TYPE, OperaType.WRITE.toString)
        .option(NebulaOptions.USER_NAME, connectionConfig.getUser)
        .option(NebulaOptions.AUTHOPTIONS, connectionConfig.getAuthOptions)
        .option(NebulaOptions.GRAPH_ADDRESS, connectionConfig.getGraphAddress)
        .option(NebulaOptions.TIMEOUT, connectionConfig.getTimeout)
        .option(NebulaOptions.EXECUTION_RETRY, connectionConfig.getExecRetry)
        .option(NebulaOptions.EXECUTION_RETRY_INTERVAL, connectionConfig.getExecRetryIntervalMs)
        .option(NebulaOptions.GRAPH_NAME, writeConfig.getGraphName)
        .option(NebulaOptions.LABEL, writeConfig.getEdgeType)
        .option(NebulaOptions.SRC_PK_FIELD, writeConfig.getSrcPkFields)
        .option(NebulaOptions.DST_PK_FIELD, writeConfig.getDstPkFields)
        .option(NebulaOptions.BATCH_SIZE, writeConfig.getBatchSize)
        .option(NebulaOptions.SRC_PK_AS_PROP, writeConfig.getSrcAsProp)
        .option(NebulaOptions.DST_PK_AS_PROP, writeConfig.getDstAsProp)
        .option(NebulaOptions.WRITE_MODE, writeConfig.getWriteMode)
        .option(NebulaOptions.DISABLE_WRITE_LOG, writeConfig.isDisableWriteLog)
        .option(NebulaOptions.ERROR_WHEN_FAILED, writeConfig.throwErrorWhenFailed)
        .option(NebulaOptions.ENABLE_TLS, connectionConfig.getEnableTls)
        .option(NebulaOptions.DISABLE_VERIFY_SERVER_CERT, connectionConfig.getDisableVerifyServerCert)
        .option(NebulaOptions.TLS_CA, connectionConfig.getTlsCaPath)
        .option(NebulaOptions.TLS_CERT, connectionConfig.getTlsCertPath)
        .option(NebulaOptions.TLS_KEY, connectionConfig.getTlsKeyPath)

      if (writeConfig.getSchema != null) {
        dfWriter.option(NebulaOptions.SCHEMA, writeConfig.getSchema)
      }
      if (writeConfig.getDateFormat != null) {
        dfWriter.option(NebulaOptions.DATE_FORMAT, writeConfig.getDateFormat)
      }
      if (writeConfig.getZonedDateTimeFormat != null) {
        dfWriter.option(NebulaOptions.ZONED_DATETIME_FORMAT, writeConfig.getZonedDateTimeFormat)
      }
      if (writeConfig.getLocalDateTimeFormat != null) {
        dfWriter.option(NebulaOptions.LOCAL_DATETIME_FORMAT, writeConfig.getLocalDateTimeFormat)
      }
      if (writeConfig.getZonedTimeFormat != null) {
        dfWriter.option(NebulaOptions.ZONED_TIME_FORMAT, writeConfig.getZonedTimeFormat)
      }
      if (writeConfig.getLocalTimeFormat != null) {
        dfWriter.option(NebulaOptions.LOCAL_TIME_FORMAT, writeConfig.getLocalTimeFormat)
      }
      dfWriter.save()
    }
  }


  /**
   * spark reader for nebula graph
   */
  implicit class NebulaDataFrameReader(reader: DataFrameReader) {
    var connectionConfig: NebulaConnectionConfig = _
    var readConfig      : ReadNebulaConfig       = _

    def nebula(connectionConfig: NebulaConnectionConfig,
               readConfig: ReadNebulaConfig): NebulaDataFrameReader = {
      SparkValidate.validate("3.*.*")
      this.connectionConfig = connectionConfig
      this.readConfig = readConfig
      this
    }

    /**
     * Reading nodes from Nebula Graph
     *
     * @return DataFrame
     */
    def loadNode(): DataFrame = {
      assert(connectionConfig != null && readConfig != null,
             "nebula config is not set, please call nebula() before loadNode")
      val dfReader = reader
        .format(classOf[NebulaDataSource].getName)
        .option(NebulaOptions.TYPE, DataTypeEnum.NODE.toString)
        .option(NebulaOptions.OPERATE_TYPE, OperaType.READ.toString)
        .option(NebulaOptions.TIMEOUT, connectionConfig.getTimeout)
        .option(NebulaOptions.EXECUTION_RETRY, connectionConfig.getExecRetry)
        .option(NebulaOptions.EXECUTION_RETRY_INTERVAL, connectionConfig.getExecRetryIntervalMs)
        .option(NebulaOptions.GRAPH_ADDRESS, connectionConfig.getGraphAddress)
        .option(NebulaOptions.USER_NAME, connectionConfig.getUser)
        .option(NebulaOptions.AUTHOPTIONS, connectionConfig.getAuthOptions)
        .option(NebulaOptions.GRAPH_NAME, readConfig.getGraphName)
        .option(NebulaOptions.LABEL, readConfig.getTypeName)
        .option(NebulaOptions.BATCH_SIZE, readConfig.getBatchSize)
        .option(NebulaOptions.PARTITION_NUMBER, readConfig.getPartitionNum)
        .option(NebulaOptions.ENABLE_TLS, connectionConfig.getEnableTls)
        .option(NebulaOptions.DISABLE_VERIFY_SERVER_CERT, connectionConfig.getDisableVerifyServerCert)
        .option(NebulaOptions.TLS_CA, connectionConfig.getTlsCaPath)
        .option(NebulaOptions.TLS_CERT, connectionConfig.getTlsCertPath)
        .option(NebulaOptions.TLS_KEY, connectionConfig.getTlsKeyPath)

      if (readConfig.getReturnCols != null) {
        dfReader.option(NebulaOptions.RETURN_COLS, readConfig.getReturnColsString)
      }
      if (readConfig.getSchema != null) {
        dfReader.option(NebulaOptions.SCHEMA, readConfig.getSchema)
      }

      dfReader.load()
    }

    /**
     * Reading edges from Nebula Graph
     *
     * @return DataFrame
     */
    def loadEdge(): DataFrame = {
      assert(connectionConfig != null && readConfig != null,
             "nebula config is not set, please call nebula() before loadEdgesToDF")

      val dfReader = reader
        .format(classOf[NebulaDataSource].getName)
        .option(NebulaOptions.TYPE, DataTypeEnum.EDGE.toString)
        .option(NebulaOptions.OPERATE_TYPE, OperaType.READ.toString)
        .option(NebulaOptions.TIMEOUT, connectionConfig.getTimeout)
        .option(NebulaOptions.EXECUTION_RETRY, connectionConfig.getExecRetry)
        .option(NebulaOptions.EXECUTION_RETRY_INTERVAL, connectionConfig.getExecRetryIntervalMs)
        .option(NebulaOptions.GRAPH_ADDRESS, connectionConfig.getGraphAddress)
        .option(NebulaOptions.USER_NAME, connectionConfig.getUser)
        .option(NebulaOptions.AUTHOPTIONS, connectionConfig.getAuthOptions)
        .option(NebulaOptions.GRAPH_NAME, readConfig.getGraphName)
        .option(NebulaOptions.LABEL, readConfig.getTypeName)
        .option(NebulaOptions.BATCH_SIZE, readConfig.getBatchSize)
        .option(NebulaOptions.PARTITION_NUMBER, readConfig.getPartitionNum)
        .option(NebulaOptions.ENABLE_TLS, connectionConfig.getEnableTls)
        .option(NebulaOptions.DISABLE_VERIFY_SERVER_CERT, connectionConfig.getDisableVerifyServerCert)
        .option(NebulaOptions.TLS_CA, connectionConfig.getTlsCaPath)
        .option(NebulaOptions.TLS_CERT, connectionConfig.getTlsCertPath)
        .option(NebulaOptions.TLS_KEY, connectionConfig.getTlsKeyPath)
      if (readConfig.getReturnCols != null) {
        dfReader.option(NebulaOptions.RETURN_COLS, readConfig.getReturnColsString)
      }
      if (readConfig.getSchema != null) {
        dfReader.option(NebulaOptions.SCHEMA, readConfig.getSchema)
      }
      dfReader.load()
    }
  }


  /**
   * spark reader for nebula gql query
   */
  implicit class NebulaDataFrameGqlReader(reader: DataFrameReader) {
    var connectionConfig: NebulaConnectionConfig = _
    var gqlConfig       : GqlNebulaConfig        = _

    def gql(connectionConfig: NebulaConnectionConfig, gqlConfig: GqlNebulaConfig): NebulaDataFrameGqlReader = {
      SparkValidate.validate("3.*.*")
      this.connectionConfig = connectionConfig
      this.gqlConfig = gqlConfig
      this
    }

    /**
     * Reading gql result from NebulaGraph
     *
     * @return DataFrame
     */
    def load(): DataFrame = {
      assert(connectionConfig != null && gqlConfig != null,
             "nebula gql config is not set, please call gql() before load()")
      val dfReader = reader
        .format(classOf[NebulaDataSource].getName)
        .option(NebulaOptions.TYPE, DataTypeEnum.GQL.toString)
        .option(NebulaOptions.OPERATE_TYPE, OperaType.READ.toString)
        .option(NebulaOptions.TIMEOUT, connectionConfig.getTimeout)
        .option(NebulaOptions.EXECUTION_RETRY, connectionConfig.getExecRetry)
        .option(NebulaOptions.EXECUTION_RETRY_INTERVAL, connectionConfig.getExecRetryIntervalMs)
        .option(NebulaOptions.GRAPH_ADDRESS, connectionConfig.getGraphAddress)
        .option(NebulaOptions.USER_NAME, connectionConfig.getUser)
        .option(NebulaOptions.AUTHOPTIONS, connectionConfig.getAuthOptions)
        .option(NebulaOptions.ENABLE_TLS, connectionConfig.getEnableTls)
        .option(NebulaOptions.DISABLE_VERIFY_SERVER_CERT, connectionConfig.getDisableVerifyServerCert)
        .option(NebulaOptions.TLS_CA, connectionConfig.getTlsCaPath)
        .option(NebulaOptions.TLS_CERT, connectionConfig.getTlsCertPath)
        .option(NebulaOptions.TLS_KEY, connectionConfig.getTlsKeyPath)
        .option(NebulaOptions.GQL, gqlConfig.getGql)
      if (gqlConfig.getSchema != null) {
        dfReader.option(NebulaOptions.SCHEMA, gqlConfig.getSchema)
      }
      dfReader.load()
    }
  }


}
