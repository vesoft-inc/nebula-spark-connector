/*
 * Copyright (c) 2025 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

package com.vesoft.nebula.spark.common.writer

import com.google.common.util.concurrent.RateLimiter
import com.vesoft.nebula.driver.graph.data.ResultSet
import com.vesoft.nebula.spark.common.NebulaOptions
import com.vesoft.nebula.spark.common.nebula.GraphProvider
import org.slf4j.LoggerFactory

import java.util.concurrent.TimeUnit
import scala.collection.mutable.ListBuffer

class NebulaWriter(nebulaOptions: NebulaOptions) extends Serializable {
  protected val LOG = LoggerFactory.getLogger(this.getClass)

  val failedExecs: ListBuffer[String] = new ListBuffer[String]

  val graphProvider = new GraphProvider(nebulaOptions)

  def submit(exec: String): ResultSet = {
    @transient val rateLimiter = RateLimiter.create(nebulaOptions.rateLimit)
    if (rateLimiter.tryAcquire(30, TimeUnit.MINUTES)) {
      val result = graphProvider.submit(exec)
      result
    } else {
      throw new RuntimeException("get rateLimiter timeout.")
    }
  }
}
