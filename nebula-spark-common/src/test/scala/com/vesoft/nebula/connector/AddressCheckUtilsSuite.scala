/* Copyright (c) 2023 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

package com.vesoft.nebula.connector

import com.vesoft.nebula.connector.utils.AddressCheckUtil
import org.scalatest.funsuite.AnyFunSuite

class AddressCheckUtilsSuite extends AnyFunSuite {

  test("checkAddress") {
    var addr        = "127.0.0.1:9669"
    var hostAddress = AddressCheckUtil.getAddressFromString(addr)
    assert("127.0.0.1".equals(hostAddress._1))
    assert(hostAddress._2 == 9669)

    addr = "localhost:9669"
    hostAddress = AddressCheckUtil.getAddressFromString(addr)
    assert("localhost".equals(hostAddress._1))

    addr = "www.baidu.com:22"
    hostAddress = AddressCheckUtil.getAddressFromString(addr)
    assert(hostAddress._2 == 22)

    addr = "[2023::2]:65535"
    hostAddress = AddressCheckUtil.getAddressFromString(addr)
    assert(hostAddress._2 == 65535)

    addr = "2023::3"
    hostAddress = AddressCheckUtil.getAddressFromString(addr)
    assert(hostAddress._1.equals("2023::3"))
    assert(hostAddress._2 == -1)

    // bad address
    addr = "localhost:65536"
    assertThrows[IllegalArgumentException](AddressCheckUtil.getAddressFromString(addr))
    addr = "localhost:-1"
    assertThrows[IllegalArgumentException](AddressCheckUtil.getAddressFromString(addr))
    addr = "[localhost]:9669"
    assertThrows[IllegalArgumentException](AddressCheckUtil.getAddressFromString(addr))
    addr = "www.baidu.com:+25"
    assertThrows[IllegalArgumentException](AddressCheckUtil.getAddressFromString(addr))
    addr = "[]:8080"
    assertThrows[IllegalArgumentException](AddressCheckUtil.getAddressFromString(addr))
  }
}
