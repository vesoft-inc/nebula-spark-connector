/* Copyright (c) 2023 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

package com.vesoft.nebula.connector.utils

import com.google.common.base.Strings

object AddressCheckUtil {

  def getAddressFromString(addr: String): (String, Int) = {
    if (addr == null) {
      throw new IllegalArgumentException("wrong address format.")
    }

    val (host, portString) =
      if (addr.startsWith("[")) {
        getHostAndPortFromBracketedHost(addr)
      } else if (addr.count(_ == ':') == 1) {
        val array = addr.split(":", 2)
        (array(0), array(1))
      } else {
        (addr, null)
      }

    val port = getPort(portString, addr)
    (host, port)
  }

  private def getPort(portString: String, addr: String): Int =
    if (Strings.isNullOrEmpty(portString)) {
      -1
    } else {
      require(portString.forall(_.isDigit), s"Port must be numeric: $addr")
      val port = portString.toInt
      require(1 <= port && port <= 65535, s"Port number out of range: $addr")
      port
    }

  def getHostAndPortFromBracketedHost(addr: String): (String, String) = {
    val colonIndex        = addr.indexOf(":")
    val closeBracketIndex = addr.lastIndexOf("]")
    if (colonIndex < 0 || closeBracketIndex < colonIndex) {
      throw new IllegalArgumentException(s"invalid bracketed host/port: $addr")
    }
    val host: String = addr.substring(1, closeBracketIndex)
    if (closeBracketIndex + 1 == addr.length) {
      return (host, "")
    } else {
      if (addr.charAt(closeBracketIndex + 1) != ':') {
        throw new IllegalArgumentException(s"only a colon may follow a close bracket: $addr")
      }
      for (i <- closeBracketIndex + 2 until addr.length) {
        if (!Character.isDigit(addr.charAt(i))) {
          throw new IllegalArgumentException(s"Port must be numeric: $addr")
        }
      }
    }
    (host, addr.substring(closeBracketIndex + 2))
  }

}
