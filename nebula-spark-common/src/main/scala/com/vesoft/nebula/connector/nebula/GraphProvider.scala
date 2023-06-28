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
import com.vesoft.nebula.connector.Address
import com.vesoft.nebula.connector.exception.GraphConnectException
import com.vesoft.nebula.connector.ssl.{CASSLSignParams, SSLSignType, SelfSSLSignParams}
import org.apache.log4j.Logger

import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer

/**
  * GraphProvider for Nebula Graph Service
  */
class GraphProvider(addresses: List[Address],
                    user: String,
                    password: String,
                    timeout: Int,
                    enableSSL: Boolean = false,
                    sslSignType: String = null,
                    caSignParam: CASSLSignParams = null,
                    selfSignParam: SelfSSLSignParams = null)
    extends AutoCloseable
    with Serializable {
  @transient private[this] lazy val LOG = Logger.getLogger(this.getClass)

  @transient val nebulaPoolConfig = new NebulaPoolConfig

  @transient val pool: NebulaPool = new NebulaPool
  val address                     = addresses.map { case (host, port) => new HostAddress(host, port) }
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
  val randAddr = scala.util.Random.shuffle(address)
  pool.init(randAddr.asJava, nebulaPoolConfig)

  lazy val session: Session = pool.getSession(user, password, true)

  /**
    * release session
    */
  def releaseGraphClient(): Unit =
    session.release()

  override def close(): Unit = {
    releaseGraphClient()
    pool.close()
  }

  /**
    * switch space
    *
    * @param user
    * @param password
    * @param space
    * @return if execute succeed
    */
  def switchSpace(space: String): Boolean = {
    val switchStatment = s"use $space"
    LOG.info(s"switch space $space")
    val result = submit(switchStatment)
    if (!result.isSucceeded) {
      LOG.error(s"switch space $space failed, ${result.getErrorMessage}")
      throw new RuntimeException(s"switch space $space failed, ${result.getErrorMessage}")
    }
    true
  }

  /**
    * execute the statement
    *
    * @param statement insert tag/edge statement
    * @return execute result
    */
  def submit(statement: String): ResultSet =
    session.execute(statement)
}
