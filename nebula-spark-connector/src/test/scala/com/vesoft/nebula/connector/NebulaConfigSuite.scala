/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 *  This source code is licensed under Apache 2.0 License,
 *  attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

package com.vesoft.nebula.connector

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
      .withEnableGraphSsl(true)
      .withEnableMetaSsl(true)
      .withSslSignType(SslSignType.CA)
      .withCaSslSignParam("cacrtFile", "crtFile", "keyFile")
      .build()
  }

  test("test correct ssl config with no sign type param") {
    assertThrows[AssertionError](
      NebulaConnectionConfig
        .builder()
        .withMetaAddress("127.0.0.1:9559")
        .withGraphAddress("127.0.0.1:9669")
        .withEnableGraphSsl(true)
        .withEnableMetaSsl(true)
        .withCaSslSignParam("cacrtFile", "crtFile", "keyFile")
        .build())
  }

  test("test correct ssl config with wrong ca param") {
    assertThrows[AssertionError](
      NebulaConnectionConfig
        .builder()
        .withMetaAddress("127.0.0.1:9559")
        .withGraphAddress("127.0.0.1:9669")
        .withEnableGraphSsl(true)
        .withEnableMetaSsl(true)
        .withSslSignType(SslSignType.CA)
        .withSelfSslSignParam("crtFile", "keyFile", "password")
        .build())
  }

  test("test correct ssl config with wrong self param") {
    assertThrows[AssertionError](
      NebulaConnectionConfig
        .builder()
        .withMetaAddress("127.0.0.1:9559")
        .withGraphAddress("127.0.0.1:9669")
        .withEnableGraphSsl(true)
        .withEnableMetaSsl(true)
        .withSslSignType(SslSignType.SELF)
        .withCaSslSignParam("cacrtFile", "crtFile", "keyFile")
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
