/* Copyright (c) 2022 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

package com.vesoft.nebula.connector.reader

import com.vesoft.nebula.client.graph.data.{
  CASignedSSLParam,
  HostAddress,
  SSLParam,
  SelfSignedSSLParam,
  ValueWrapper
}
import com.vesoft.nebula.client.storage.StorageClient
import com.vesoft.nebula.client.storage.data.BaseTableRow
import com.vesoft.nebula.client.storage.scan.{
  ScanEdgeResult,
  ScanEdgeResultIterator,
  ScanVertexResult,
  ScanVertexResultIterator
}
import com.vesoft.nebula.connector.NebulaUtils.NebulaValueGetter
import com.vesoft.nebula.connector.exception.GraphConnectException
import com.vesoft.nebula.connector.nebula.MetaProvider
import com.vesoft.nebula.connector.ssl.SSLSignType
import com.vesoft.nebula.connector.{NebulaOptions, NebulaUtils}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.SpecificInternalRow
import org.apache.spark.sql.types.StructType
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.collection.mutable.ListBuffer

trait NebulaReader {
  private val LOG: Logger = LoggerFactory.getLogger(this.getClass)

  private var metaProvider: MetaProvider = _
  private var schema      : StructType   = _

  protected var dataIterator    : Iterator[BaseTableRow]           = _
  protected var scanPartIterator: Iterator[Int]                    = _
  protected var resultValues    : mutable.ListBuffer[List[Object]] = mutable.ListBuffer[List[Object]]()
  protected var storageClient   : StorageClient                    = _
  protected var nebulaOptions   : NebulaOptions                    = _

  private var vertexResponseIterator: ScanVertexResultIterator = _
  private var edgeResponseIterator  : ScanEdgeResultIterator   = _

  /**
   * init the reader: init metaProvider, storageClient
   */
  def init(index: Int, nebulaOptions: NebulaOptions, schema: StructType): Int = {
    this.schema = schema
    this.nebulaOptions = nebulaOptions

    metaProvider = new MetaProvider(
      nebulaOptions.getMetaAddress,
      nebulaOptions.timeout,
      nebulaOptions.connectionRetry,
      nebulaOptions.executionRetry,
      nebulaOptions.enableMetaSSL,
      nebulaOptions.sslSignType,
      nebulaOptions.caSignParam,
      nebulaOptions.selfSignParam
      )
    val address: ListBuffer[HostAddress] = new ListBuffer[HostAddress]

    for (addr <- nebulaOptions.getMetaAddress) {
      address.append(new HostAddress(addr._1, addr._2))
    }

    var sslParam: SSLParam = null
    if (nebulaOptions.enableStorageSSL) {
      SSLSignType.withName(nebulaOptions.sslSignType) match {
        case SSLSignType.CA => {
          val caSSLSignParams = nebulaOptions.caSignParam
          sslParam = new CASignedSSLParam(caSSLSignParams.caCrtFilePath,
                                          caSSLSignParams.crtFilePath,
                                          caSSLSignParams.keyFilePath)
        }
        case SSLSignType.SELF => {
          val selfSSLSignParams = nebulaOptions.selfSignParam
          sslParam = new SelfSignedSSLParam(selfSSLSignParams.crtFilePath,
                                            selfSSLSignParams.keyFilePath,
                                            selfSSLSignParams.password)
        }
        case _ => throw new IllegalArgumentException("ssl sign type is not supported")
      }
      this.storageClient = new StorageClient(address.asJava,
                                             nebulaOptions.timeout,
                                             nebulaOptions.connectionRetry,
                                             nebulaOptions.executionRetry,
                                             true,
                                             sslParam)
    } else {
      this.storageClient = new StorageClient(address.asJava, nebulaOptions.timeout)
    }
    storageClient.setUser(nebulaOptions.user)
    storageClient.setPassword(nebulaOptions.passwd)
    if (nebulaOptions.storageAddressMapping != null) {
      storageClient.setStorageAddressMapping(nebulaOptions.storageAddressMapping.asJava)
    }

    if (!storageClient.connect()) {
      throw new GraphConnectException("storage connect failed.")
    }
    // allocate scanPart to this partition
    val totalPart = metaProvider.getPartitionNumber(nebulaOptions.spaceName)
    totalPart
  }

  /**
   * resolve the vertex/edge data to InternalRow
   */
  protected def getRow(): InternalRow = {
    val resultSet: Array[ValueWrapper]      =
      dataIterator.next().getValues.toArray.map(v => v.asInstanceOf[ValueWrapper])
    val getters  : Array[NebulaValueGetter] = NebulaUtils.makeGetters(schema)
    val mutableRow                          = new SpecificInternalRow(schema.fields.map(x => x.dataType))

    for (i <- getters.indices) {
      val value: ValueWrapper = resultSet(i)
      var resolved            = false
      if (value.isNull) {
        mutableRow.setNullAt(i)
        resolved = true
      }
      if (value.isString) {
        getters(i).apply(value.asString(), mutableRow, i)
        resolved = true
      }
      if (value.isDate) {
        getters(i).apply(value.asDate(), mutableRow, i)
        resolved = true
      }
      if (value.isTime) {
        getters(i).apply(value.asTime(), mutableRow, i)
        resolved = true
      }
      if (value.isDateTime) {
        getters(i).apply(value.asDateTime(), mutableRow, i)
        resolved = true
      }
      if (value.isLong) {
        getters(i).apply(value.asLong(), mutableRow, i)
      }
      if (value.isBoolean) {
        getters(i).apply(value.asBoolean(), mutableRow, i)
      }
      if (value.isDouble) {
        getters(i).apply(value.asDouble(), mutableRow, i)
      }
      if (value.isGeography) {
        getters(i).apply(value.asGeography(), mutableRow, i)
      }
      if (value.isDuration) {
        getters(i).apply(value.asDuration(), mutableRow, i)
      }
    }
    mutableRow
  }

  /**
   * if the scan response has next vertex
   */
  protected def hasNextVertexRow: Boolean = {
    (dataIterator != null || vertexResponseIterator != null || scanPartIterator.hasNext) && {
      var continue: Boolean = false
      var break   : Boolean = false
      while ((dataIterator == null || !dataIterator.hasNext) && !break) {
        resultValues.clear()
        continue = false
        if (vertexResponseIterator == null || !vertexResponseIterator.hasNext) {
          if (scanPartIterator.hasNext) {
            try {
              if (nebulaOptions.noColumn) {
                vertexResponseIterator = storageClient.scanVertex(nebulaOptions.spaceName,
                                                                  scanPartIterator.next(),
                                                                  nebulaOptions.label,
                                                                  nebulaOptions.limit,
                                                                  0,
                                                                  Long.MaxValue,
                                                                  false,
                                                                  false)
              } else {
                vertexResponseIterator =
                  storageClient.scanVertex(nebulaOptions.spaceName,
                                           scanPartIterator.next(),
                                           nebulaOptions.label,
                                           nebulaOptions.getReturnCols.asJava,
                                           nebulaOptions.limit,
                                           0,
                                           Long.MaxValue,
                                           false,
                                           false)
              }
            } catch {
              case e: Exception =>
                LOG.error(s"Exception scanning vertex ${nebulaOptions.label}", e)
                storageClient.close()
                throw new Exception(e.getMessage, e)
            }
            // jump to the next loop
            continue = true
          }
          // break while loop
          break = !continue
        } else {
          val next: ScanVertexResult = vertexResponseIterator.next
          if (!next.isEmpty) {
            dataIterator = next.getVertexTableRows.iterator().asScala
          }
        }
      }

      dataIterator != null && dataIterator.hasNext
    }
  }

  /**
   * if the scan response has next edge
   */
  protected def hasNextEdgeRow: Boolean =
    (dataIterator != null || edgeResponseIterator != null || scanPartIterator.hasNext) && {
      var continue: Boolean = false
      var break   : Boolean = false
      while ((dataIterator == null || !dataIterator.hasNext) && !break) {
        resultValues.clear()
        continue = false
        if (edgeResponseIterator == null || !edgeResponseIterator.hasNext) {
          if (scanPartIterator.hasNext) {
            try {
              if (nebulaOptions.noColumn) {
                edgeResponseIterator = storageClient.scanEdge(nebulaOptions.spaceName,
                                                              scanPartIterator.next(),
                                                              nebulaOptions.label,
                                                              nebulaOptions.limit,
                                                              0L,
                                                              Long.MaxValue,
                                                              false,
                                                              false)
              } else {
                edgeResponseIterator = storageClient.scanEdge(nebulaOptions.spaceName,
                                                              scanPartIterator.next(),
                                                              nebulaOptions.label,
                                                              nebulaOptions.getReturnCols.asJava,
                                                              nebulaOptions.limit,
                                                              0,
                                                              Long.MaxValue,
                                                              false,
                                                              false)
              }
            } catch {
              case e: Exception =>
                LOG.error(s"Exception scanning vertex ${nebulaOptions.label}", e)
                storageClient.close()
                throw new Exception(e.getMessage, e)
            }
            // jump to the next loop
            continue = true
          }
          // break while loop
          break = !continue
        } else {
          val next: ScanEdgeResult = edgeResponseIterator.next
          if (!next.isEmpty) {
            dataIterator = next.getEdgeTableRows.iterator().asScala
          }
        }
      }

      dataIterator != null && dataIterator.hasNext
    }

  /**
   * close the reader
   */
  protected def closeReader(): Unit = {
    metaProvider.close()
    storageClient.close()
  }
}
