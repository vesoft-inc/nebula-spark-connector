/*
 * Copyright (c) 2025 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

package com.vesoft.nebula.spark.common.exception


/**
  * An exception thrown if a required option is missing form [[NebulaOptions]]
  */
class IllegalOptionException(message: String, cause: Throwable = null)
    extends IllegalArgumentException(message, cause)

/**
  * An exception thrown if nebula execution occur rpc exception.
  */
class NebulaRPCException(message: String, cause: Throwable = null)
    extends RuntimeException(message, cause)
