/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

package com.vesoft.nebula.connector.nebula

import com.vesoft.nebula.client.graph.data.{
  CASignedSSLParam,
  HostAddress,
  SSLParam,
  SelfSignedSSLParam
}
import com.vesoft.nebula.client.meta.MetaClient
import com.vesoft.nebula.connector.connector.Address
import com.vesoft.nebula.connector.{DataTypeEnum, SSLSignType}
import com.vesoft.nebula.meta.{PropertyType, Schema}

import scala.collection.JavaConverters._
import scala.collection.mutable

class MetaProvider(addresses: List[Address],
                   timeout: Int,
                   connectionRetry: Int,
                   executionRetry: Int,
                   enableSSL: Boolean,
                   sslSignType: String = null,
                   caSignParam: CASignedSSLParam,
                   selfSignParam: SelfSignedSSLParam)
    extends AutoCloseable {

  val metaAddress        = addresses.map(address => new HostAddress(address._1, address._2)).asJava
  var client: MetaClient = null
  var sslParam: SSLParam = null
  if (enableSSL) {
    SSLSignType.withName(sslSignType) match {
      case SSLSignType.CA   => sslParam = caSignParam
      case SSLSignType.SELF => sslParam = selfSignParam
      case _                => throw new IllegalArgumentException("ssl sign type is not supported")
    }
    client = new MetaClient(metaAddress, timeout, connectionRetry, executionRetry, true, sslParam)
  } else {
    client = new MetaClient(metaAddress, timeout, connectionRetry, executionRetry)
  }
  client.connect()

  def getPartitionNumber(space: String): Int = {
    client.getPartsAlloc(space).size()
  }

  def getVidType(space: String): VidType.Value = {
    val vidType = client.getSpace(space).getProperties.getVid_type.getType
    if (vidType == PropertyType.FIXED_STRING) {
      return VidType.STRING
    }
    VidType.INT
  }

  def getTag(space: String, tag: String): Schema = {
    client.getTag(space, tag)
  }

  def getEdge(space: String, edge: String): Schema = {
    client.getEdge(space, edge)
  }

  def getTagSchema(space: String, tag: String): Map[String, Integer] = {
    val tagSchema = client.getTag(space, tag)
    val schema    = new mutable.HashMap[String, Integer]

    val columns = tagSchema.getColumns
    for (colDef <- columns.asScala) {
      schema.put(new String(colDef.getName), colDef.getType.getType.getValue)
    }
    schema.toMap
  }

  def getEdgeSchema(space: String, edge: String): Map[String, Integer] = {
    val edgeSchema = client.getEdge(space, edge)
    val schema     = new mutable.HashMap[String, Integer]

    val columns = edgeSchema.getColumns
    for (colDef <- columns.asScala) {
      schema.put(new String(colDef.getName), colDef.getType.getType.getValue)
    }
    schema.toMap
  }

  def getLabelType(space: String, label: String): DataTypeEnum.Value = {
    val tags = client.getTags(space)
    for (tag <- tags.asScala) {
      if (new String(tag.getTag_name).equals(label)) {
        return DataTypeEnum.VERTEX
      }
    }
    val edges = client.getEdges(space)
    for (edge <- edges.asScala) {
      if (new String(edge.getEdge_name).equals(label)) {
        return DataTypeEnum.EDGE
      }
    }
    null
  }

  override def close(): Unit = {
    client.close()
  }

}

object VidType extends Enumeration {
  type Type = Value

  val STRING = Value("STRING")
  val INT    = Value("INT")
}
