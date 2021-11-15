/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

package com.vesoft.nebula.connector.ssl

object SSLSignType extends Enumeration {

  type signType = Value
  val CA   = Value("ca")
  val SELF = Value("self")
}
