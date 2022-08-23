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
import com.vesoft.nebula.client.storage.data.BaseTableRow
import com.vesoft.nebula.client.storage.StorageClient
import com.vesoft.nebula.connector.{NebulaOptions, NebulaUtils, PartitionUtils}
import com.vesoft.nebula.connector.NebulaUtils.NebulaValueGetter
import com.vesoft.nebula.connector.exception.GraphConnectException
import com.vesoft.nebula.connector.nebula.MetaProvider
import com.vesoft.nebula.connector.ssl.SSLSignType
import org.apache.spark.Partition
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.SpecificInternalRow
import org.apache.spark.sql.types.StructType
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.collection.JavaConverters._

/**
  * @todo
  * iterator for nebula vertex or edge data
  * convert each vertex data or edge data to Spark SQL's Row
  */
abstract class NebulaIterator extends Iterator[InternalRow] {

  private val LOG: Logger = LoggerFactory.getLogger(classOf[NebulaIterator])

  private var metaProvider: MetaProvider = _
  private var schema: StructType         = _

  protected var dataIterator: Iterator[BaseTableRow]           = _
  protected var scanPartIterator: Iterator[Integer]            = _
  protected var resultValues: mutable.ListBuffer[List[Object]] = mutable.ListBuffer[List[Object]]()
  protected var storageClient: StorageClient                   = _

  def this(index: Partition, nebulaOptions: NebulaOptions, schema: StructType) {
    this()
    this.schema = schema

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

    if (!storageClient.connect()) {
      throw new GraphConnectException("storage connect failed.")
    }
    // allocate scanPart to this partition
    val totalPart = metaProvider.getPartitionNumber(nebulaOptions.spaceName)

    val scanParts =
      PartitionUtils.getScanParts(index.index, totalPart, nebulaOptions.partitionNums.toInt)
    LOG.info(s"partition index: ${index}, scanParts: ${scanParts.toString}")
    scanPartIterator = scanParts.iterator
  }

  /**
    * @todo
    * whether this iterator can provide another element.
    */
  override def hasNext: Boolean

  /**
    * @todo
    * Produces the next vertex or edge of this iterator.
    */
  override def next(): InternalRow = {
    val resultSet: Array[ValueWrapper] =
      dataIterator.next().getValues.toArray.map(v => v.asInstanceOf[ValueWrapper])
    val getters: Array[NebulaValueGetter] = NebulaUtils.makeGetters(schema)
    val mutableRow                        = new SpecificInternalRow(schema.fields.map(x => x.dataType))

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

}
