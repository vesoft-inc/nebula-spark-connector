/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

package com.vesoft.nebula.connector.nebula

import com.vesoft.nebula.client.graph.NebulaPoolConfig
import com.vesoft.nebula.client.graph.data.{
  CASignedSSLParam,
  HostAddress,
  ResultSet,
  SelfSignedSSLParam
}
import com.vesoft.nebula.client.graph.net.{NebulaPool, Session}
import com.vesoft.nebula.connector.connector.Address
import com.vesoft.nebula.connector.exception.GraphConnectException
import com.vesoft.nebula.connector.ssl.{CASSLSignParams, SSLSignType, SelfSSLSignParams}
import org.apache.log4j.Logger

import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer

/**
  * GraphProvider for Nebula Graph Service
  */
class GraphProvider(addresses: List[Address],
                    timeout: Int,
                    enableSSL: Boolean = false,
                    sslSignType: String = null,
                    caSignParam: CASSLSignParams = null,
                    selfSignParam: SelfSSLSignParams = null)
    extends AutoCloseable
    with Serializable {
  private[this] lazy val LOG = Logger.getLogger(this.getClass)

  @transient val nebulaPoolConfig = new NebulaPoolConfig

  @transient val pool: NebulaPool = new NebulaPool
  val address                     = new ListBuffer[HostAddress]()
  for (addr <- addresses) {
    address.append(new HostAddress(addr._1, addr._2))
  }
  nebulaPoolConfig.setMaxConnSize(1)
  nebulaPoolConfig.setTimeout(timeout)

  if (enableSSL) {
    nebulaPoolConfig.setEnableSsl(enableSSL)
    SSLSignType.withName(sslSignType) match {
      case SSLSignType.CA =>
        nebulaPoolConfig.setSslParam(
          new CASignedSSLParam(caSignParam.caCrtFilePath,
                               caSignParam.crtFilePath,
                               caSignParam.keyFilePath))
      case SSLSignType.SELF =>
        nebulaPoolConfig.setSslParam(
          new SelfSignedSSLParam(selfSignParam.crtFilePath,
                                 selfSignParam.keyFilePath,
                                 selfSignParam.password))
      case _ => throw new IllegalArgumentException("ssl sign type is not supported")
    }
  }
  pool.init(address.asJava, nebulaPoolConfig)

  var session: Session = null

  def releaseGraphClient(session: Session): Unit = {
    session.release()
  }

  override def close(): Unit = {
    pool.close()
  }

  def switchSpace(user: String, password: String, space: String): Boolean = {
    if (session == null) {
      session = pool.getSession(user, password, true)
    }
    val switchStatment = s"use $space"
    LOG.info(s"switch space $space")
    val result = submit(switchStatment)
    result.isSucceeded
  }

  def submit(statement: String): ResultSet = {
    if (session == null) {
      LOG.error("graph session is null")
      throw new GraphConnectException("session is null")
    }
    session.execute(statement)
  }
}
