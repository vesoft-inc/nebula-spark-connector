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
    var host: String       = null
    var portString: String = null

    if (addr.startsWith("[")) {
      val hostAndPort = getHostAndPortFromBracketedHost(addr)
      host = hostAndPort._1
      portString = hostAndPort._2
    } else {
      val colonPos = addr.indexOf(":")
      if (colonPos >= 0 && addr.indexOf(":", colonPos + 1) == -1) {
        host = addr.substring(0, colonPos)
        portString = addr.substring(colonPos + 1)
      } else {
        host = addr
      }
    }

    var port = -1;
    if (!Strings.isNullOrEmpty(portString)) {
      for (c <- portString.toCharArray) {
        if (!Character.isDigit(c)) {
          throw new IllegalArgumentException(s"Port must be numeric: $addr")
        }
      }
      port = Integer.parseInt(portString)
      if (port < 0 || port > 65535) {
        throw new IllegalArgumentException(s"Port number out of range: $addr")
      }
    }
    (host, port)
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
