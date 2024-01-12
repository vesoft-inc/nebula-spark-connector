/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

package com.vesoft.nebula.connector.writer

import java.util.concurrent.TimeUnit

import com.google.common.util.concurrent.RateLimiter
import com.vesoft.nebula.connector.NebulaOptions
import com.vesoft.nebula.connector.nebula.{GraphProvider, MetaProvider, VidType}
import org.slf4j.LoggerFactory

import scala.collection.mutable.ListBuffer

class NebulaWriter(nebulaOptions: NebulaOptions) extends Serializable {
  private val LOG = LoggerFactory.getLogger(this.getClass)

  val failedExecs: ListBuffer[String] = new ListBuffer[String]

  val metaProvider = new MetaProvider(
    nebulaOptions.getMetaAddress,
    nebulaOptions.timeout,
    nebulaOptions.connectionRetry,
    nebulaOptions.executionRetry,
    nebulaOptions.enableMetaSSL,
    nebulaOptions.sslSignType,
    nebulaOptions.caSignParam,
    nebulaOptions.selfSignParam
  )
  val graphProvider = new GraphProvider(
    nebulaOptions.getGraphAddress,
    nebulaOptions.user,
    nebulaOptions.passwd,
    nebulaOptions.timeout,
    nebulaOptions.enableGraphSSL,
    nebulaOptions.sslSignType,
    nebulaOptions.caSignParam,
    nebulaOptions.selfSignParam
  )
  val isVidStringType = metaProvider.getVidType(nebulaOptions.spaceName) == VidType.STRING

  def prepareSpace(): Unit = {
    graphProvider.switchSpace(nebulaOptions.spaceName)
  }

  def submit(exec: String): Unit = {
    @transient val rateLimiter = RateLimiter.create(nebulaOptions.rateLimit)
    if (rateLimiter.tryAcquire(nebulaOptions.rateTimeOut, TimeUnit.MILLISECONDS)) {
      val result = graphProvider.submit(exec)
      if (!result.isSucceeded) {
        failedExecs.append(exec)
        LOG.error(s"failed to write ${exec} for " + result.getErrorMessage)
      } else {
        LOG.info(s"batch write succeed")
        LOG.debug(s"batch write succeed: ${exec}")
      }
    } else {
      failedExecs.append(exec)
      LOG.error(s"failed to acquire reteLimiter for statement {$exec}")
    }
  }
}
