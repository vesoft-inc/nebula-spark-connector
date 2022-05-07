/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

package com.vesoft.nebula.connector

import com.vesoft.nebula.connector.ssl.{CASSLSignParams, SSLSignType, SelfSSLSignParams}
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.mutable.ListBuffer

class NebulaConnectionConfig(metaAddress: String,
                             storageAddress: String = "",
                             graphAddress: String,
                             timeout: Int,
                             connectionRetry: Int,
                             executeRetry: Int,
                             enableMetaSSL: Boolean,
                             enableGraphSSL: Boolean,
                             enableStorageSSL: Boolean,
                             signType: SSLSignType.Value,
                             caSignParam: CASSLSignParams,
                             selfSignParam: SelfSSLSignParams)
    extends Serializable {
  def getMetaAddress      = metaAddress
  def getStorageAddress   = storageAddress
  def getGraphAddress     = graphAddress
  def getTimeout          = timeout
  def getConnectionRetry  = connectionRetry
  def getExecRetry        = executeRetry
  def getEnableMetaSSL    = enableMetaSSL
  def getEnableGraphSSL   = enableGraphSSL
  def getEnableStorageSSL = enableStorageSSL
  def getSignType         = signType.toString
  def getCaSignParam: String = {
    caSignParam.caCrtFilePath + "," + caSignParam.crtFilePath + "," + caSignParam.keyFilePath
  }
  def getSelfSignParam: String = {
    selfSignParam.crtFilePath + "," + selfSignParam.keyFilePath + "," + selfSignParam.password
  }
}

object NebulaConnectionConfig {
  class ConfigBuilder {
    private val LOG = LoggerFactory.getLogger(this.getClass)

    protected var metaAddress: String  = _
    protected var storageAddress: String = ""
    protected var graphAddress: String = _
    protected var timeout: Int         = 6000
    protected var connectionRetry: Int = 1
    protected var executeRetry: Int    = 1

    protected var enableMetaSSL: Boolean           = false
    protected var enableGraphSSL: Boolean          = false
    protected var enableStorageSSL: Boolean        = false
    protected var sslSignType: SSLSignType.Value   = _
    protected var caSignParam: CASSLSignParams     = null
    protected var selfSignParam: SelfSSLSignParams = null

    /**
      * set nebula meta server address, multi addresses is split by English comma
      */
    def withMetaAddress(metaAddress: String): ConfigBuilder = {
      this.metaAddress = metaAddress
      this
    }

    /**
      * set nebula storage server address, multi addresses is split by English comma
      */
    def withStorageAddress(storageAddress: String): ConfigBuilder = {
      this.storageAddress = storageAddress
      this
    }

    /**
      * set nebula graph server address, multi addresses is split by English comma
      */
    def withGraphAddress(graphAddress: String): ConfigBuilder = {
      this.graphAddress = graphAddress
      this
    }

    /**
      * set timeout, timeout is optional
      */
    def withTimeout(timeout: Int): ConfigBuilder = {
      this.timeout = timeout
      this
    }

    /**
      * set connectionRetry, connectionRetry is optional
      */
    def withConenctionRetry(connectionRetry: Int): ConfigBuilder = {
      this.connectionRetry = connectionRetry
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
      * set enableMetaSSL, enableMetaSSL is optional
      */
    def withEnableMetaSSL(enableMetaSSL: Boolean): ConfigBuilder = {
      this.enableMetaSSL = enableMetaSSL
      this
    }

    /**
      * set enableMetaSSL, enableMetaSSL is optional
      */
    def withEnableGraphSSL(enableGraphSSL: Boolean): ConfigBuilder = {
      this.enableGraphSSL = enableGraphSSL
      this
    }

    /**
      * set enableStorageSSL, enableStorageSSL is optional
      */
    def withEnableStorageSSL(enableStorageSSL: Boolean): ConfigBuilder = {
      this.enableStorageSSL = enableStorageSSL
      this
    }

    /**
      * set ssl sign type {@link SSLSignType}
      */
    def withSSLSignType(signType: SSLSignType.Value): ConfigBuilder = {
      this.sslSignType = signType
      this
    }

    /**
      * set ca sign param for ssl
      */
    def withCaSSLSignParam(caCrtFilePath: String,
                           crtFilePath: String,
                           keyFilePath: String): ConfigBuilder = {
      this.caSignParam = CASSLSignParams(caCrtFilePath, crtFilePath, keyFilePath)
      this
    }

    /**
      * set self sign param for ssl
      */
    def withSelfSSLSignParam(crtFilePath: String,
                             keyFilePath: String,
                             password: String): ConfigBuilder = {
      this.selfSignParam = SelfSSLSignParams(crtFilePath, keyFilePath, password)
      this
    }

    /**
      * check if the connection config is valid
      */
    def check(): Unit = {
      assert(metaAddress != null && !metaAddress.isEmpty, "config address is empty.")
      assert(timeout > 0, "timeout must be larger than 0")
      assert(connectionRetry > 0 && executeRetry > 0, "retry must be larger than 0.")
      if (enableMetaSSL) {
        LOG.info("enableMetaSSL is true, then enableGraphSSL will be invalid for now.")
      }
      // check ssl param
      if (enableMetaSSL || enableGraphSSL || enableStorageSSL) {
        assert(
          !enableStorageSSL || enableStorageSSL && enableMetaSSL,
          "ssl priority order: storage > meta = graph " +
            "please make sure meta ssl is enabled when storage ssl is enabled."
        )
        sslSignType match {
          case SSLSignType.CA =>
            assert(
              caSignParam != null && caSignParam.caCrtFilePath != null
                && caSignParam.crtFilePath != null && caSignParam.keyFilePath != null,
              "ssl sign type is CA, param can not be null"
            )
          case SSLSignType.SELF =>
            assert(
              selfSignParam != null && selfSignParam.crtFilePath != null
                && selfSignParam.keyFilePath != null && selfSignParam.password != null,
              "ssl sign type is SELF, param can not be null"
            )
          case _ => assert(false, "SSLSignType config is null")
        }
      }
    }

    /**
      * build NebulaConnectionConfig
      */
    def build(): NebulaConnectionConfig = {
      check()
      new NebulaConnectionConfig(metaAddress,
                                 storageAddress,
                                 graphAddress,
                                 timeout,
                                 connectionRetry,
                                 executeRetry,
                                 enableMetaSSL,
                                 enableGraphSSL,
                                 enableStorageSSL,
                                 sslSignType,
                                 caSignParam,
                                 selfSignParam)
    }
  }

  def builder(): ConfigBuilder = {
    new ConfigBuilder
  }

}

/**
  * Base config needed when write dataframe into nebula graph
  */
private[connector] class WriteNebulaConfig(space: String,
                                           user: String,
                                           passwd: String,
                                           batch: Int,
                                           writeMode: String)
    extends Serializable {
  def getSpace     = space
  def getBatch     = batch
  def getUser      = user
  def getPasswd    = passwd
  def getWriteMode = writeMode
}

/**
  * subclass of WriteNebulaConfig to config vertex when write dataframe into nebula graph
  *
  * @param space: nebula space name
  * @param tagName: tag name
  * @param vidField: field in dataframe to indicate vertexId
  * @param vidPolicy: not required, use hash to map your vertexId
  * @param batch: amount of one batch when write into nebula graph
  */
class WriteNebulaVertexConfig(space: String,
                              tagName: String,
                              vidField: String,
                              vidPolicy: String,
                              batch: Int,
                              vidAsProp: Boolean,
                              user: String,
                              passwd: String,
                              writeMode: String)
    extends WriteNebulaConfig(space, user, passwd, batch, writeMode) {
  def getTagName   = tagName
  def getVidField  = vidField
  def getVidPolicy = if (vidPolicy == null) "" else vidPolicy
  def getVidAsProp = vidAsProp
}

/**
  * object WriteNebulaVertexConfig
  * */
object WriteNebulaVertexConfig {

  private val LOG: Logger = LoggerFactory.getLogger(this.getClass)

  class WriteVertexConfigBuilder {

    var space: String     = _
    var tagName: String   = _
    var vidPolicy: String = _
    var vidField: String  = _
    var batch: Int        = 512
    var user: String      = "root"
    var passwd: String    = "nebula"
    var writeMode: String = "insert"

    /** whether set vid as property */
    var vidAsProp: Boolean = false

    /**
      * set space name
      */
    def withSpace(space: String): WriteVertexConfigBuilder = {
      this.space = space
      this
    }

    /**
      * set tag name
      */
    def withTag(tagName: String): WriteVertexConfigBuilder = {
      this.tagName = tagName
      this
    }

    /**
      * set which field in dataframe as nebula tag's id
      */
    def withVidField(vidField: String): WriteVertexConfigBuilder = {
      this.vidField = vidField
      this
    }

    /**
      * set vid policy, its optional
      * only "hash" and "uuid" is validate
      */
    def withVidPolicy(vidPolicy: String): WriteVertexConfigBuilder = {
      this.vidPolicy = vidPolicy
      this
    }

    /**
      * set data amount for one batch, default is 512
      */
    def withBatch(batch: Int): WriteVertexConfigBuilder = {
      this.batch = batch
      this
    }

    /**
      * set whether vid as prop, default is false
      */
    def withVidAsProp(vidAsProp: Boolean): WriteVertexConfigBuilder = {
      this.vidAsProp = vidAsProp
      this
    }

    /**
      * set user name for nebula graph
      */
    def withUser(user: String): WriteVertexConfigBuilder = {
      this.user = user
      this
    }

    /**
      * set password for nebula graph's user
      */
    def withPasswd(passwd: String): WriteVertexConfigBuilder = {
      this.passwd = passwd
      this
    }

    /**
      * set nebula write mode for nebula tag, INSERT or UPDATE
      */
    def withWriteMode(writeMode: WriteMode.Value): WriteVertexConfigBuilder = {
      this.writeMode = writeMode.toString
      this
    }

    /**
      * check and get WriteNebulaVertexConfig
      */
    def build(): WriteNebulaVertexConfig = {
      check()
      new WriteNebulaVertexConfig(space,
                                  tagName,
                                  vidField,
                                  vidPolicy,
                                  batch,
                                  vidAsProp,
                                  user,
                                  passwd,
                                  writeMode)
    }

    private def check(): Unit = {
      assert(space != null && !space.isEmpty, s"config space is empty.")

      assert(vidField != null && !vidField.isEmpty, "config vidField is empty.")
      assert(batch > 0, s"config batch must be positive, your batch is $batch.")
      assert(
        vidPolicy == null
          || vidPolicy.equalsIgnoreCase(KeyPolicy.HASH.toString)
          || vidPolicy.equalsIgnoreCase(KeyPolicy.UUID.toString),
        "config vidPolicy is illegal, please don't set vidPolicy or set vidPolicy \"HASH\" or \"UUID\""
      )
      assert(user != null && !user.isEmpty, "user is empty")
      assert(passwd != null && !passwd.isEmpty, "passwd is empty")
      try {
        WriteMode.withName(writeMode.toLowerCase())
      } catch {
        case e: Throwable =>
          assert(false, s"optional write mode: insert or update, your write mode is $writeMode")
      }
      if (writeMode.equalsIgnoreCase(WriteMode.UPDATE.toString)) {
        assert(batch <= 512, "the maximum number of statements for Nebula is 512")
      }
      if (!writeMode.equalsIgnoreCase(WriteMode.DELETE.toString)) {
        assert(tagName != null && !tagName.isEmpty, s"config tagName is empty.")
      } else {
        if (tagName == null) tagName = "tag" // set a default for delete mode, happy to pass the option check.
      }
      LOG.info(
        s"NebulaWriteVertexConfig={space=$space,tagName=$tagName,vidField=$vidField," +
          s"vidPolicy=$vidPolicy,batch=$batch,writeMode=$writeMode}")
    }
  }

  def builder(): WriteVertexConfigBuilder = {
    new WriteVertexConfigBuilder
  }
}

/**
  * subclass of WriteNebulaConfig to config edge when write dataframe into nebula graph
  *
  * @param space: nebula space name
  * @param edgeName: edge name
  * @param srcFiled: field in dataframe to indicate src vertex id
  * @param srcPolicy: not required, use hash to map your src vertex id
  * @param dstField: field in dataframe to indicate dst vertex id
  * @param dstPolicy: not required, use hash to map your dst vertex id
  * @param rankField: not required, field in dataframe to indicate edge rank
  * @param batch: amount of one batch when write into nebula graph
  */
class WriteNebulaEdgeConfig(space: String,
                            edgeName: String,
                            srcFiled: String,
                            srcPolicy: String,
                            dstField: String,
                            dstPolicy: String,
                            rankField: String,
                            batch: Int,
                            srcAsProp: Boolean,
                            dstAsProp: Boolean,
                            rankAsProp: Boolean,
                            user: String,
                            passwd: String,
                            writeMode: String)
    extends WriteNebulaConfig(space, user, passwd, batch, writeMode) {
  def getEdgeName  = edgeName
  def getSrcFiled  = srcFiled
  def getSrcPolicy = if (srcPolicy == null) "" else srcPolicy
  def getDstField  = dstField
  def getDstPolicy = if (dstPolicy == null) "" else dstPolicy
  def getRankField = if (rankField == null) "" else rankField

  def getSrcAsProp  = srcAsProp
  def getDstAsProp  = dstAsProp
  def getRankAsProp = rankAsProp

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

    var space: String    = _
    var edgeName: String = _

    var srcIdField: String = _
    var srcPolicy: String  = _
    var dstIdField: String = _
    var dstPolicy: String  = _
    var rankField: String  = _
    var batch: Int         = 512
    var user: String       = "root"
    var passwd: String     = "nebula"

    /** whether srcId as property */
    var srcAsProp: Boolean = false

    /** whether dstId as property */
    var dstAsProp: Boolean = false

    /** whether set rank as property */
    var rankAsProp: Boolean = false

    /** write mode for nebula, insert or update */
    var writeMode: String = WriteMode.INSERT.toString

    /**
      * set space name
      */
    def withSpace(space: String): WriteEdgeConfigBuilder = {
      this.space = space
      this
    }

    /**
      * set edge type name
      */
    def withEdge(edgeName: String): WriteEdgeConfigBuilder = {
      this.edgeName = edgeName
      this
    }

    /**
      * set rank field in dataframe
      * it rankField is not set, then edge has default 0 rank value
      * */
    def withRankField(rankField: String): WriteEdgeConfigBuilder = {
      this.rankField = rankField
      this
    }

    /**
      * set which field in dataframe as nebula edge's src id
      */
    def withSrcIdField(srcIdField: String): WriteEdgeConfigBuilder = {
      this.srcIdField = srcIdField
      this
    }

    /**
      * set policy for edge src id, its optional
      */
    def withSrcPolicy(srcPolicy: String): WriteEdgeConfigBuilder = {
      this.srcPolicy = srcPolicy
      this
    }

    /**
      * set which field in dataframe as nebula edge's dst id
      */
    def withDstIdField(dstIdField: String): WriteEdgeConfigBuilder = {
      this.dstIdField = dstIdField
      this
    }

    /**
      * set policy for edge dst id, its optional
      */
    def withDstPolicy(dstPolicy: String): WriteEdgeConfigBuilder = {
      this.dstPolicy = dstPolicy
      this
    }

    /**
      * set data amount for one batch, default is 512
      */
    def withBatch(batch: Int): WriteEdgeConfigBuilder = {
      this.batch = batch
      this
    }

    /**
      * set whether src id as property
      */
    def withSrcAsProperty(srcAsProp: Boolean): WriteEdgeConfigBuilder = {
      this.srcAsProp = srcAsProp
      this
    }

    /**
      * set whether dst id as property
      */
    def withDstAsProperty(dstAsProp: Boolean): WriteEdgeConfigBuilder = {
      this.dstAsProp = dstAsProp
      this
    }

    /**
      * set whether rank as property
      */
    def withRankAsProperty(rankAsProp: Boolean): WriteEdgeConfigBuilder = {
      this.rankAsProp = rankAsProp
      this
    }

    /**
      * set user name for nebula graph
      */
    def withUser(user: String): WriteEdgeConfigBuilder = {
      this.user = user
      this
    }

    /**
      * set password for nebula graph's user
      */
    def withPasswd(passwd: String): WriteEdgeConfigBuilder = {
      this.passwd = passwd
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
      * check configs and get WriteNebulaEdgeConfig
      */
    def build(): WriteNebulaEdgeConfig = {
      check()
      new WriteNebulaEdgeConfig(space,
                                edgeName,
                                srcIdField,
                                srcPolicy,
                                dstIdField,
                                dstPolicy,
                                rankField,
                                batch,
                                srcAsProp,
                                dstAsProp,
                                rankAsProp,
                                user,
                                passwd,
                                writeMode)
    }

    private def check(): Unit = {
      assert(space != null && !space.isEmpty, s"config space is empty.")

      assert(srcIdField != null && !srcIdField.isEmpty, "config srcIdField is empty.")
      assert(dstIdField != null && !dstIdField.isEmpty, "config dstIdField is empty.")
      assert(
        srcPolicy == null
          || srcPolicy.equalsIgnoreCase(KeyPolicy.HASH.toString)
          || srcPolicy.equalsIgnoreCase(KeyPolicy.UUID.toString),
        "config srcPolicy is illegal, please don't set srcPolicy or set srcPolicy \"HASH\" or \"UUID\""
      )
      assert(
        dstPolicy == null
          || dstPolicy.equalsIgnoreCase(KeyPolicy.HASH.toString)
          || dstPolicy.equalsIgnoreCase(KeyPolicy.UUID.toString),
        "config dstPolicy is illegal, please don't set dstPolicy or set dstPolicy \"HASH\" or \"UUID\""
      )
      assert(batch > 0, s"config batch must be positive, your batch is $batch.")
      assert(user != null && !user.isEmpty, "user is empty")
      assert(passwd != null && !passwd.isEmpty, "passwd is empty")
      try {
        WriteMode.withName(writeMode.toLowerCase)
      } catch {
        case e: Throwable =>
          assert(false, s"optional write mode: insert or update, your write mode is $writeMode")
      }
      if (writeMode.equalsIgnoreCase(WriteMode.UPDATE.toString)) {
        assert(batch <= 512, "the maximum number of statements for Nebula is 512")
      }
      assert(edgeName != null && !edgeName.isEmpty, s"config edgeName is empty.")
      LOG.info(
        s"NebulaWriteEdgeConfig={space=$space,edgeName=$edgeName,srcField=$srcIdField," +
          s"srcPolicy=$srcPolicy，dstField=$dstIdField,dstPolicy=$dstPolicy,rankField=$rankField," +
          s"writeMode=$writeMode}")
    }
  }

  def builder(): WriteEdgeConfigBuilder = {
    new WriteEdgeConfigBuilder
  }
}

/**
  * config needed when read from nebula graph
  *    for read vertex or edge:
  *    you must need to set these configs: addresses/space/label
  *    you can set noColumn to true to read no vertex col, and you can set returnCols to read the specific cols, if the returnCols is empty, then read all the columns.
  *    you can set partitionNum to define spark partition nums to read nebula graph.
  */
class ReadNebulaConfig(space: String,
                       label: String,
                       returnCols: List[String],
                       noColumn: Boolean,
                       partitionNum: Int,
                       limit: Int)
    extends Serializable {
  def getSpace        = space
  def getLabel        = label
  def getReturnCols   = returnCols
  def getNoColumn     = noColumn
  def getPartitionNum = partitionNum
  def getLimit        = limit
  // todo add filter
}

/**
  * object ReadNebulaConfig
  */
object ReadNebulaConfig {
  private val LOG: Logger = LoggerFactory.getLogger(this.getClass)

  class ReadConfigBuilder {
    var space: String                  = _
    var label: String                  = _
    var returnCols: ListBuffer[String] = new ListBuffer[String]
    var noColumn: Boolean              = false
    var partitionNum: Int              = 100
    var limit: Int                     = 1000

    def withSpace(space: String): ReadConfigBuilder = {
      this.space = space
      this
    }
    def withLabel(label: String): ReadConfigBuilder = {
      this.label = label
      this
    }

    def withReturnCols(returnCols: List[String]): ReadConfigBuilder = {
      for (col: String <- returnCols) {
        this.returnCols.append(col)
      }
      this
    }

    /**
      * if noColumn is set to true, then returnCols is no need and it will be invalidate even if configured
      */
    def withNoColumn(noColumn: Boolean): ReadConfigBuilder = {
      this.noColumn = noColumn
      this
    }

    /**
      * set partition num for spark, default is 100
      */
    def withPartitionNum(partitionNum: Int): ReadConfigBuilder = {
      this.partitionNum = partitionNum
      this
    }

    /**
      * set limit for scan nebula graph, default is 1000
      */
    def withLimit(limit: Int): ReadConfigBuilder = {
      this.limit = limit
      this
    }

    def build(): ReadNebulaConfig = {
      check()
      new ReadNebulaConfig(space, label, returnCols.toList, noColumn, partitionNum, limit)
    }

    private def check(): Unit = {
      assert(space != null && !space.isEmpty, s"config space is empty.")
      assert(label != null && !label.isEmpty, s"config label is empty.")
      assert(limit > 0, s"config limit must be positive, your limit is $limit")
      assert(partitionNum > 0, s"config partitionNum must be positive, your partitionNum is $limit")
      if (noColumn && returnCols.nonEmpty) {
        LOG.warn(
          s"noColumn is true, returnCols will be invalidate "
            + s"and your result will not contain property for $label")
      }
      if (!noColumn && returnCols.isEmpty) {
        LOG.warn(s"returnCols is empty and your result will contain all properties for $label")
      }
      LOG.info(
        s"NebulaReadConfig={space=$space,label=$label,returnCols=${returnCols.toList},"
          + s"noColumn=$noColumn,partitionNum=$partitionNum}")
    }
  }

  def builder(): ReadConfigBuilder = {
    new ReadConfigBuilder
  }

}
