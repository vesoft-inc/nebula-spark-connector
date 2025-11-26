/*
 * Copyright (c) 2025 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

package com.vesoft.nebula.spark.common

import com.alibaba.fastjson.JSONObject
import com.alibaba.fastjson.serializer.JSONObjectCodec
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

class NebulaConnectionConfig(graphAddress: String,
                             user: String,
                             passwd: String,
                             authOptions: Map[String, Any],
                             timeout: Int,
                             executeRetry: Int,
                             executeRetryIntervalMs: Int,
                             enableTls: Boolean,
                             disableVerifyServerCert: Boolean,
                             tlsCaPath: String,
                             tlsCertPath: String,
                             tlsKeyPath: String)
  extends Serializable {
  def getGraphAddress = graphAddress

  def getUser = user


  def getAuthOptions = {
    val newAuthOptions = new mutable.HashMap[String, Any]()
    if (passwd != null) {
      newAuthOptions.put("password", passwd)
    }
    newAuthOptions ++= authOptions
    val json = new JSONObject
    newAuthOptions.foreach(x => json.put(x._1, x._2))
    json.toJSONString
  }

  def getTimeout = timeout

  def getExecRetry = executeRetry

  def getExecRetryIntervalMs = executeRetryIntervalMs

  def getEnableTls: Boolean = enableTls

  def getDisableVerifyServerCert: Boolean = disableVerifyServerCert

  def getTlsCaPath: String = tlsCaPath

  def getTlsCertPath: String = tlsCertPath

  def getTlsKeyPath: String = tlsKeyPath

}

object NebulaConnectionConfig {
  class ConfigBuilder {
    private val LOG = LoggerFactory.getLogger(this.getClass)

    protected var graphAddress           : String  = _
    protected var user                   : String  = _
    protected var passwd                 : String  = _
    protected var authOptions                      = new mutable.HashMap[String, Any]
    protected var timeout                : Int     = 5
    protected var executeRetry           : Int     = 3
    protected var executeRetryIntervalMs : Int     = 0
    protected var enableTls              : Boolean = false
    protected var disableVerifyServerCert: Boolean = false
    protected var tlsCaPath              : String  = ""
    protected var tlsCertPath            : String  = ""
    protected var tlsKeyPath             : String  = ""

    /**
     * set nebula graph server address, multi addresses is split by English comma
     */
    def withGraphAddress(graphAddress: String): ConfigBuilder = {
      this.graphAddress = graphAddress
      this
    }

    /**
     * set user name for nebula graph
     */
    def withUser(user: String): ConfigBuilder = {
      this.user = user
      this
    }

    /**
     * set password for nebula graph's user
     */
    def withPasswd(passwd: String): ConfigBuilder = {
      this.passwd = passwd
      this
    }

    /**
     * set auth options for nebula graph's user
     */
    def withAuthOptions(authOptions: Map[String, Any]): ConfigBuilder = {
      if (authOptions != null) {
        this.authOptions ++= authOptions
      }
      this
    }

    /**
     * set timeout, timeout is optional， unit: second
     */
    def withTimeoutSec(timeout: Int): ConfigBuilder = {
      this.timeout = timeout
      this
    }

    /**
     * set executeRetry, executeRetry is optional
     */
    def withExecuteRetry(executeRetry: Int): ConfigBuilder = {
      this.executeRetry = executeRetry
      this
    }

    /**
     * set executeRetryIntervalMs, executeRetryIntervalMs is optional
     */
    def withExecuteRetryIntervalMs(executeRetryIntervalMs: Int): ConfigBuilder = {
      this.executeRetryIntervalMs = executeRetryIntervalMs
      this
    }

    /**
     * enable the tls
     * */
    def withEnableTls(): ConfigBuilder = {
      this.enableTls = true
      this
    }

    /**
     * disable the verification of server cert
     */
    def withDisableVerifyServerCert(): ConfigBuilder = {
      this.disableVerifyServerCert = true
      this
    }

    /**
     * set the path of tls ca
     */
    def withTlsCaPath(tlsCaPath: String): ConfigBuilder = {
      this.tlsCaPath = tlsCaPath;
      this
    }

    /**
     * set the path of client private key
     */
    def withTlsKeyPath(tlsKeyPath: String): ConfigBuilder = {
      this.tlsKeyPath = tlsKeyPath
      this
    }

    /**
     * set the path of tls cert
     */
    def withTlsCertPath(tlsCertPath: String): ConfigBuilder = {
      this.tlsCertPath = tlsCertPath
      this
    }

    /**
     * check if the connection config is valid
     */
    def check(): Unit = {
      assert(graphAddress != null && graphAddress.nonEmpty, "graph address cannot be blank.")
      assert(user != null && user.nonEmpty, "user cannot be blank.")
      assert((passwd != null && passwd.nonEmpty) || authOptions.nonEmpty,
             "password and authOptions cannot be blank at the same time.")
      assert(timeout > 0, "timeout must be larger than 0.")
      assert(executeRetry >= 0, "retry must be equal or larger than 0.")
      assert(!enableTls || !disableVerifyServerCert || tlsCaPath.nonEmpty,
             "tlsCaPath cannot be blank when enable tls and not disable verify server cert")
    }

    /**
     * build NebulaConnectionConfig
     */
    def build(): NebulaConnectionConfig = {
      check()
      new NebulaConnectionConfig(graphAddress,
                                 user,
                                 passwd,
                                 authOptions.toMap,
                                 timeout,
                                 executeRetry,
                                 executeRetryIntervalMs,
                                 enableTls,
                                 disableVerifyServerCert,
                                 tlsCaPath,
                                 tlsCertPath,
                                 tlsKeyPath)
    }
  }

  def builder(): ConfigBuilder = {
    new ConfigBuilder
  }

}

/**
 * Base config needed when write dataframe into nebula graph
 */
class WriteNebulaConfig(graphName: String,
                        schema: String,
                        dateFormat: String,
                        zonedDateTimeFormat: String,
                        localDateTimeFormat: String,
                        zonedTimeFormat: String,
                        localTimeFormat: String,
                        batchSize: Int,
                        writeMode: String,
                        disableWriteLog: Boolean)
  extends Serializable {
  def getGraphName: String = graphName

  def getSchema: String = schema

  def getDateFormat: String = dateFormat

  def getZonedDateTimeFormat: String = zonedDateTimeFormat

  def getLocalDateTimeFormat: String = localDateTimeFormat

  def getZonedTimeFormat: String = zonedTimeFormat

  def getLocalTimeFormat: String = localTimeFormat

  def getBatchSize: Int = batchSize

  def getWriteMode: String = writeMode

  def isDisableWriteLog: Boolean = disableWriteLog
}

/**
 * subclass of WriteNebulaConfig to config node when write dataframe into nebula graph
 *
 * @param graphName       : nebula graph name
 * @param nodeType        : node type name
 * @param batchSize       : amount of one batch when write into nebula graph
 * @param writeMode       : write mode, insert / update / delete
 * @param disableWriteLog : disable the log print for write result, such as batch size and latency
 */
class WriteNebulaNodeConfig(graphName: String,
                            schema: String,
                            dateFormat: String,
                            zonedDateTimeFormat: String,
                            localDateTimeFormat: String,
                            zonedTimeFormat: String,
                            localTimeFormat: String,
                            nodeType: String,
                            batchSize: Int,
                            writeMode: String,
                            disableWriteLog: Boolean)
  extends WriteNebulaConfig(graphName,
                            schema,
                            dateFormat,
                            zonedDateTimeFormat,
                            localDateTimeFormat,
                            zonedTimeFormat,
                            localTimeFormat,
                            batchSize,
                            writeMode,
                            disableWriteLog) {
  def getNodeType = nodeType

}

/**
 * object WriteNebulaNodeConfig
 * */
object WriteNebulaNodeConfig {

  private val LOG: Logger = LoggerFactory.getLogger(this.getClass)

  class WriteNodeConfigBuilder {
    private var graphName          : String  = _
    private var schema             : String  = _
    private var dateFormat         : String  = _
    private var zonedDateTimeFormat: String  = _
    private var localDateTimeFormat: String  = _
    private var zonedTimeFormat    : String  = _
    private var localTimeFormat    : String  = _
    private var nodeType           : String  = _
    private var writeMode          : String  = "insert"
    private var disableWriteLog    : Boolean = false
    private var batchSize          : Int     = 512

    /**
     * set graph name
     */
    def withGraphName(graphName: String): WriteNodeConfigBuilder = {
      this.graphName = graphName
      this
    }

    /**
     * set schema path
     */
    def withSchema(schema: String): WriteNodeConfigBuilder = {
      this.schema = schema
      this
    }

    /**
     * set date format
     */
    def withDateFormat(dateFormat: String): WriteNodeConfigBuilder = {
      this.dateFormat = dateFormat;
      this
    }

    /**
     * set zoned datetime format
     */
    def withZonedDatetimeFormat(zonedDatetimeFormat: String): WriteNodeConfigBuilder = {
      this.zonedDateTimeFormat = zonedDatetimeFormat;
      this
    }

    /**
     * set local datetime format
     */
    def withLocalDatetimeFormat(localDatetimeFormat: String): WriteNodeConfigBuilder = {
      this.localDateTimeFormat = localDatetimeFormat;
      this
    }

    /**
     * set zoned time format
     */
    def withZonedTimeFormat(zonedTimeFormat: String): WriteNodeConfigBuilder = {
      this.zonedTimeFormat = zonedTimeFormat;
      this
    }

    /**
     * set local time format
     */
    def withLocalTimeFormat(localTimeFormat: String): WriteNodeConfigBuilder = {
      this.localTimeFormat = localTimeFormat;
      this
    }

    /**
     * set tag name
     */
    def withNodeType(nodeType: String): WriteNodeConfigBuilder = {
      this.nodeType = nodeType
      this
    }

    /**
     * set data amount for one batch, default is 512
     */
    def withBatchSize(batchSize: Int): WriteNodeConfigBuilder = {
      this.batchSize = batchSize
      this
    }

    /**
     * set nebula write mode for nebula tag, INSERT or UPDATE
     */
    def withWriteMode(writeMode: WriteMode.Value): WriteNodeConfigBuilder = {
      this.writeMode = writeMode.toString
      this
    }

    /**
     * set disableWriteLog, default is false
     */
    def withDisableWriteLog(disableWriteLog: Boolean): WriteNodeConfigBuilder = {
      this.disableWriteLog = disableWriteLog
      this
    }

    /**
     * check and get WriteNebulaNodeConfig
     */
    def build(): WriteNebulaNodeConfig = {
      check()
      new WriteNebulaNodeConfig(graphName,
                                schema,
                                dateFormat,
                                zonedDateTimeFormat,
                                localDateTimeFormat,
                                zonedTimeFormat,
                                localTimeFormat,
                                nodeType,
                                batchSize,
                                writeMode,
                                disableWriteLog)
    }

    /**
     * check the validation for {@link WriteNebulaNodeConfig}
     */
    private def check(): Unit = {
      assert(graphName != null && graphName.nonEmpty, s"config graphName can not be empty.")
      assert(nodeType != null && nodeType.nonEmpty, "config nodeType can not be empty")
      assert(batchSize > 0, s"config batchSize must be positive, your batchSize is $batchSize.")

      try {
        WriteMode.withName(writeMode.toLowerCase())
      } catch {
        case e: Throwable =>
          assert(false, s"optional write mode: insert or update, your write mode is $writeMode")
      }

      LOG.info(
        s"NebulaWriteNodeConfig={graphName=$graphName,nodeType=$nodeType," +
          s"batchSize=$batchSize,writeMode=$writeMode,disableWriteLog=$disableWriteLog}")
    }
  }

  def builder(): WriteNodeConfigBuilder = {
    new WriteNodeConfigBuilder
  }
}

/**
 * subclass of WriteNebulaConfig to config edge when write dataframe into nebula graph
 *
 * @param graphName       : nebula graph name
 * @param edgeType        : edge name
 * @param srcPkFiled      : field in dataframe to indicate src node primary key
 * @param dstPkField      : field in dataframe to indicate dst node primary key
 * @param batchSize       : amount of one batch when write into nebula graph
 * @param srcPkAsProp     : whether use src node primary key as edge's property
 * @param dstPkAsProp     : whether use dst node primary key as edge's property
 * @param writeMode       : write mode, insert / update / delete
 * @param disableWriteLog : disable the log print for write result, such as batch size and latency
 */
class WriteNebulaEdgeConfig(graphName: String,
                            schema: String,
                            dateFormat: String,
                            zonedDateTimeFormat: String,
                            localDateTimeFormat: String,
                            zonedTimeFormat: String,
                            localTimeFormat: String,
                            edgeType: String,
                            srcPkFields: List[String],
                            dstPkFields: List[String],
                            batchSize: Int,
                            srcPkAsProp: Boolean,
                            dstPkAsProp: Boolean,
                            writeMode: String,
                            disableWriteLog: Boolean)
  extends WriteNebulaConfig(graphName,
                            schema,
                            dateFormat,
                            zonedDateTimeFormat,
                            localDateTimeFormat,
                            zonedTimeFormat,
                            localTimeFormat,
                            batchSize,
                            writeMode,
                            disableWriteLog) {
  def getEdgeType: String = edgeType

  def getSrcPkFields: String = srcPkFields.mkString("&&")

  def getDstPkFields: String = dstPkFields.mkString("&&")

  def getSrcAsProp: Boolean = srcPkAsProp

  def getDstAsProp: Boolean = dstPkAsProp
}

/**
 * object WriteNebulaEdgeConfig
 */
object WriteNebulaEdgeConfig {

  private val LOG: Logger = LoggerFactory.getLogger(WriteNebulaEdgeConfig.getClass)

  /**
   * a builder to create {@link WriteNebulaEdgeConfig}
   */
  class WriteEdgeConfigBuilder {
    private var graphName          : String  = _
    private var schema             : String  = _
    private var dateFormat         : String  = _
    private var zonedDateTimeFormat: String  = _
    private var localDateTimeFormat: String  = _
    private var zonedTimeFormat    : String  = _
    private var localTimeFormat    : String  = _
    private var writeMode          : String  = WriteMode.INSERT.toString
    private var disableWriteLog    : Boolean = false

    private var edgeType    : String             = _
    private var srcPkFields : ListBuffer[String] = new ListBuffer[String]
    private var dstPkFields : ListBuffer[String] = new ListBuffer[String]
    private var srcPksAsProp: Boolean            = false
    private var dstPksAsProp: Boolean            = false
    private var batchSize   : Int                = 512

    /**
     * set graph name
     */
    def withGraphName(graphName: String): WriteEdgeConfigBuilder = {
      this.graphName = graphName
      this
    }

    /**
     * set schema path
     */
    def withSchema(schema: String): WriteEdgeConfigBuilder = {
      this.schema = schema
      this
    }

    /**
     * set date format
     */
    def withDateFormat(dateFormat: String): WriteEdgeConfigBuilder = {
      this.dateFormat = dateFormat;
      this
    }

    /**
     * set zoned datetime format
     */
    def withZonedDatetimeFormat(zonedDatetimeFormat: String): WriteEdgeConfigBuilder = {
      this.zonedDateTimeFormat = zonedDatetimeFormat;
      this
    }

    /**
     * set local datetime format
     */
    def withLocalDatetimeFormat(localDatetimeFormat: String): WriteEdgeConfigBuilder = {
      this.localDateTimeFormat = localDatetimeFormat;
      this
    }

    /**
     * set zoned time format
     */
    def withZonedTimeFormat(zonedTimeFormat: String): WriteEdgeConfigBuilder = {
      this.zonedTimeFormat = zonedTimeFormat;
      this
    }

    /**
     * set local time format
     */
    def withLocalTimeFormat(localTimeFormat: String): WriteEdgeConfigBuilder = {
      this.localTimeFormat = localTimeFormat;
      this
    }

    /**
     * set edge type name
     */
    def withEdge(edgeType: String): WriteEdgeConfigBuilder = {
      this.edgeType = edgeType
      this
    }

    /**
     * set which field in dataframe as nebula edge's src node's primary key
     * use this method when src node's pk is just one property
     */
    def withSrcPkField(srcPkField: String): WriteEdgeConfigBuilder = {
      this.srcPkFields.append(srcPkField)
      this
    }

    /**
     * set which field in dataframe as nebula edge's src node's primary key
     * use this method when src node's pk is multiple properties
     *
     * @param srcPkFields list of src node primary keys property
     * @return WriteEdgeConfigBuilder
     */
    def withSrcPkFields(srcPkFields: List[String]): WriteEdgeConfigBuilder = {
      this.srcPkFields.appendAll(srcPkFields)
      this
    }

    /**
     * set which field in dataframe as nebula edge's dst primary key
     * use this method when dst node's pk is just one property
     */
    def withDstPkField(dstIdField: String): WriteEdgeConfigBuilder = {
      this.dstPkFields.append(dstIdField)
      this
    }

    /**
     * set which field in dataframe as nebula edge's dst id
     * use this method when dst node's pk is multiple properties
     */
    def withDstPkFields(dstIdFields: List[String]): WriteEdgeConfigBuilder = {
      this.dstPkFields.appendAll(dstIdFields)
      this
    }

    /**
     * set data amount for one batch, default is 512
     */
    def withBatchSize(batchSize: Int): WriteEdgeConfigBuilder = {
      this.batchSize = batchSize
      this
    }

    /**
     * set whether src id as property
     */
    def withSrcPksAsProperty(srcPksAsProp: Boolean): WriteEdgeConfigBuilder = {
      this.srcPksAsProp = srcPksAsProp
      this
    }

    /**
     * set whether dst id as property
     */
    def withDstPksAsProperty(dstPksAsProp: Boolean): WriteEdgeConfigBuilder = {
      this.dstPksAsProp = dstPksAsProp
      this
    }

    /**
     * set write mode for nebula edge, INSERT or UPDATE
     */
    def withWriteMode(writeMode: WriteMode.Value): WriteEdgeConfigBuilder = {
      this.writeMode = writeMode.toString
      this
    }

    /**
     * set disableWriteLog, default is false
     */
    def withDisableWriteLog(disableWriteLog: Boolean): WriteEdgeConfigBuilder = {
      this.disableWriteLog = disableWriteLog
      this
    }

    /**
     * check configs and get WriteNebulaEdgeConfig
     */
    def build(): WriteNebulaEdgeConfig = {
      check()
      new WriteNebulaEdgeConfig(graphName,
                                schema,
                                dateFormat,
                                zonedDateTimeFormat,
                                localDateTimeFormat,
                                zonedTimeFormat,
                                localTimeFormat,
                                edgeType,
                                srcPkFields.toList,
                                dstPkFields.toList,
                                batchSize,
                                srcPksAsProp,
                                dstPksAsProp,
                                writeMode,
                                disableWriteLog)
    }

    private def check(): Unit = {
      assert(graphName != null && graphName.nonEmpty, s"config graphName can not be empty.")
      assert(edgeType != null && edgeType.nonEmpty, "config edgeType can not be empty")
      assert(srcPkFields != null && srcPkFields.nonEmpty, "config srcPkFields can not be empty.")
      assert(dstPkFields != null && dstPkFields.nonEmpty, "config dstPkFields can not be empty.")
      assert(batchSize > 0, s"config batchSize must be positive, your batchSize is $batchSize.")

      try {
        WriteMode.withName(writeMode.toLowerCase)
      } catch {
        case e: Throwable =>
          assert(false, s"optional write mode: insert or update, your write mode is $writeMode")
      }
      // the batch size must be 1 for DELETE edge
      if (writeMode.equalsIgnoreCase(WriteMode.DELETE.toString)) {
        LOG.info("the write mode is DELETE for edge, batch size is automatically adjusted to 1")
        batchSize = 1
      }
      LOG.info(
        s"NebulaWriteEdgeConfig={graphName=$graphName,edgeType=$edgeType,srcPkFields=$srcPkFields," +
          s"dstPkFields=$dstPkFields,batchSize=$batchSize,srcPkAsProp=$srcPksAsProp," +
          s"dstPkAsProp=$dstPksAsProp,writeMode=$writeMode,disableWriteLog=$disableWriteLog}")
    }
  }

  def builder(): WriteEdgeConfigBuilder = {
    new WriteEdgeConfigBuilder
  }
}


class ReadNebulaConfig(schema: String,
                       graphName: String,
                       typeName: String,
                       returnCols: ListBuffer[String],
                       partitionNum: Int,
                       batchSize: Int) extends Serializable {
  def getSchema: String = schema

  def getGraphName: String = graphName

  def getTypeName: String = typeName

  def getReturnCols: ListBuffer[String] = returnCols

  def getReturnColsString: String = returnCols.mkString(",")

  def getPartitionNum: Int = partitionNum

  def getBatchSize: Int = batchSize
}


/**
 * config for reading NebulaGraph
 */
object ReadNebulaConfig {
  private val LOG: Logger = LoggerFactory.getLogger(this.getClass)

  def builder(): ReadConfigBuilder = {
    new ReadConfigBuilder
  }

  class ReadConfigBuilder {
    private var schema      : String             = null
    private var graphName   : String             = _
    private var typeName    : String             = _
    private var returnCols  : ListBuffer[String] = _
    private var partitionNum: Int                = 10
    private var batchSize   : Int                = 2000

    /**
     * config the schema path for reading
     */
    def withSchema(schema: String): ReadConfigBuilder = {
      this.schema = schema
      this
    }

    /**
     * config the graph name for reading
     */
    def withGraphName(graphName: String): ReadConfigBuilder = {
      this.graphName = graphName
      this
    }

    /**
     * config the type name for reading
     */
    def withTypeName(typeName: String): ReadConfigBuilder = {
      this.typeName = typeName
      this
    }

    /**
     * config the property names for reading
     * if you want to read all the properties, please ignore withReturnCols, or config returnCols as null.
     */
    def withReturnCols(returnCols: List[String]): ReadConfigBuilder = {
      if (returnCols != null) {
        this.returnCols = new ListBuffer[String]
        for (col: String <- returnCols) {
          this.returnCols.append(col)
        }
      }
      this
    }


    /**
     * config the partition num for spark, default is 10
     */
    def withPartitionNum(partitionNum: Int): ReadConfigBuilder = {
      this.partitionNum = partitionNum
      this
    }

    /**
     * config the batch size for each scan request, default is 2000
     */
    def withBatchSize(batchSize: Int): ReadConfigBuilder = {
      this.batchSize = batchSize
      this
    }


    /**
     * build a ReadNebulaConfig
     */
    def build(): ReadNebulaConfig = {
      check()
      new ReadNebulaConfig(schema, graphName, typeName, returnCols, partitionNum, batchSize)
    }

    /**
     * check the validation for configs
     */
    private def check(): Unit = {
      assert(graphName != null && graphName.nonEmpty, "config graphName can't be empty.")
      assert(typeName != null && typeName.nonEmpty, "config typeName can't be empty.")
      assert(partitionNum > 0, s"config partitionNum must be positive, your value is $partitionNum.")
      assert(batchSize > 0, s"config batchSize must be positive, your value is $batchSize.")
    }
  }
}

class GqlNebulaConfig(schema: String, gql: String) extends Serializable {
  def getSchema: String = schema

  def getGql: String = gql
}

/**
 * config for reading NebulaGraph through gql
 */
object GqlNebulaConfig {
  private val LOG: Logger = LoggerFactory.getLogger(this.getClass)

  def builder(): GqlConfigBuilder = {
    new GqlConfigBuilder
  }

  class GqlConfigBuilder {
    private var schema: String = null
    private var gql   : String = _

    def withSchema(schema: String): GqlConfigBuilder = {
      this.schema = schema
      this
    }

    def withGql(gql: String): GqlConfigBuilder = {
      this.gql = gql;
      this
    }


    /**
     * build a ReadNebulaConfig
     */
    def build(): GqlNebulaConfig = {
      check()
      new GqlNebulaConfig(schema, gql)
    }

    private def check(): Unit = {
      assert(gql != null, "config gql can't be empty.")
    }
  }
}

