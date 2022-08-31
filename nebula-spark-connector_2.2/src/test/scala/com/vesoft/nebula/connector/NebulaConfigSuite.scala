/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

package com.vesoft.nebula.connector

import com.vesoft.nebula.connector.ssl.SSLSignType
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

class NebulaConfigSuite extends AnyFunSuite with BeforeAndAfterAll {

  test("test NebulaConnectionConfig") {

    assertThrows[AssertionError](NebulaConnectionConfig.builder().withTimeout(1).build())

    assertThrows[AssertionError](NebulaConnectionConfig.builder().withTimeout(-1).build())

    NebulaConnectionConfig
      .builder()
      .withMetaAddress("127.0.0.1:9559")
      .withTimeout(1)
      .build()
  }

  test("test correct ssl config") {
    NebulaConnectionConfig
      .builder()
      .withMetaAddress("127.0.0.1:9559")
      .withGraphAddress("127.0.0.1:9669")
      .withEnableGraphSSL(true)
      .withEnableMetaSSL(true)
      .withSSLSignType(SSLSignType.CA)
      .withCaSSLSignParam("cacrtFile", "crtFile", "keyFile")
      .build()
  }

  test("test correct ssl config with wrong ssl priority") {
    assertThrows[AssertionError](
      NebulaConnectionConfig
        .builder()
        .withMetaAddress("127.0.0.1:9559")
        .withGraphAddress("127.0.0.1:9669")
        .withEnableStorageSSL(true)
        .withEnableMetaSSL(false)
        .withSSLSignType(SSLSignType.CA)
        .withCaSSLSignParam("caCrtFile", "crtFile", "keyFile")
        .build())
  }

  test("test correct ssl config with no sign type param") {
    assertThrows[AssertionError](
      NebulaConnectionConfig
        .builder()
        .withMetaAddress("127.0.0.1:9559")
        .withGraphAddress("127.0.0.1:9669")
        .withEnableGraphSSL(true)
        .withEnableMetaSSL(true)
        .withCaSSLSignParam("caCrtFile", "crtFile", "keyFile")
        .build())
  }

  test("test correct ssl config with wrong ca param") {
    assertThrows[AssertionError](
      NebulaConnectionConfig
        .builder()
        .withMetaAddress("127.0.0.1:9559")
        .withGraphAddress("127.0.0.1:9669")
        .withEnableGraphSSL(true)
        .withEnableMetaSSL(true)
        .withSSLSignType(SSLSignType.CA)
        .withSelfSSLSignParam("crtFile", "keyFile", "password")
        .build())
  }

  test("test correct ssl config with wrong self param") {
    assertThrows[AssertionError](
      NebulaConnectionConfig
        .builder()
        .withMetaAddress("127.0.0.1:9559")
        .withGraphAddress("127.0.0.1:9669")
        .withEnableGraphSSL(true)
        .withEnableMetaSSL(true)
        .withSSLSignType(SSLSignType.SELF)
        .withCaSSLSignParam("cacrtFile", "crtFile", "keyFile")
        .build())
  }

  test("test WriteNebulaConfig") {
    var writeNebulaConfig: WriteNebulaVertexConfig = null

    writeNebulaConfig = WriteNebulaVertexConfig
      .builder()
      .withSpace("test")
      .withTag("tag")
      .withVidField("vid")
      .build()

    assert(!writeNebulaConfig.getVidAsProp)
    assert(writeNebulaConfig.getSpace.equals("test"))
  }

  test("wrong batch size for update") {
    assertThrows[AssertionError](
      WriteNebulaVertexConfig
        .builder()
        .withSpace("test")
        .withTag("tag")
        .withVidField("vId")
        .withWriteMode(WriteMode.UPDATE)
        .withBatch(513)
        .build())
    assertThrows[AssertionError](
      WriteNebulaEdgeConfig
        .builder()
        .withSpace("test")
        .withEdge("edge")
        .withSrcIdField("src")
        .withDstIdField("dst")
        .withWriteMode(WriteMode.UPDATE)
        .withBatch(513)
        .build())
  }

  test("test wrong policy") {
    assertThrows[AssertionError](
      WriteNebulaVertexConfig
        .builder()
        .withSpace("test")
        .withTag("tag")
        .withVidField("vId")
        .withVidPolicy("wrong_policy")
        .build())
  }

  test("test wrong batch") {
    assertThrows[AssertionError](
      WriteNebulaVertexConfig
        .builder()
        .withSpace("test")
        .withTag("tag")
        .withVidField("vId")
        .withVidPolicy("hash")
        .withBatch(-1)
        .build())
  }

  test("test ReadNebulaConfig") {
    ReadNebulaConfig
      .builder()
      .withSpace("test")
      .withLabel("tagName")
      .withNoColumn(true)
      .withReturnCols(List("col"))
      .build()
  }

}
