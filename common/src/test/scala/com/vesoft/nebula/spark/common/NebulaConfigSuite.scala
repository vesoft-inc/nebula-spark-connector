/*
 * Copyright (c) 2025 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

package com.vesoft.nebula.spark.common

import org.apache.spark.sql.catalyst.util.CaseInsensitiveMap
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

class NebulaConfigSuite extends AnyFunSuite with BeforeAndAfterAll {
  test("test NebulaConnectionConfig") {
    val config = NebulaConnectionConfig
      .builder()
      .withGraphAddress("127.0.0.1:9669")
      .withUser("root")
      .withPasswd("nebula")
      .withTimeoutSec(1)
      .withExecuteRetry(2)
      .withExecuteRetryIntervalMs(1000)
      .build()
    assert(config.getGraphAddress.equals("127.0.0.1:9669"))
    assert(config.getUser.equals("root"))
    assert(config.getAuthOptions.contains("password"))
    assert(config.getAuthOptions.contains("nebula"))
    assert(config.getExecRetry == 2)
    assert(config.getExecRetryIntervalMs == 1000)
    assert(config.getTimeout == 1)
  }

  test("test empty auth options") {
    assertThrows[AssertionError](NebulaConnectionConfig
      .builder()
      .withGraphAddress("127.0.0.1:9669")
      .withUser("root")
      .withTimeoutSec(1)
      .withExecuteRetry(2)
      .withExecuteRetryIntervalMs(1000)
      .build())
  }

  test("test empty password config") {
    val config = NebulaConnectionConfig
      .builder()
      .withGraphAddress("127.0.0.1:9669")
      .withUser("root")
      .withPasswd("nebula")
      .withAuthOptions(Map("password" -> "nebula"))
      .build()
    assert(config.getGraphAddress.equals("127.0.0.1:9669"))
    assert(config.getUser.equals("root"))
    assert(config.getAuthOptions.contains("password"))
    assert(config.getAuthOptions.contains("nebula"))
  }

  test("test auth options json escaping and round trip") {
    val config = NebulaConnectionConfig
      .builder()
      .withGraphAddress("127.0.0.1:9669")
      .withUser("root")
      .withAuthOptions(Map("password" -> "ne\"bu\\la\n", "retry" -> 3, "enabled" -> true))
      .build()

    val json    = config.getAuthOptions
    val parsed  = AuthOptionsJson.parseObject(json)

    assert(json.contains("\\\""))
    assert(json.contains("\\\\"))
    assert(json.contains("\\n"))
    assert(parsed.get("password") == "ne\"bu\\la\n")
    assert(parsed.get("retry") == Int.box(3))
    assert(parsed.get("enabled") == Boolean.box(true))
  }

  test("test NebulaOptions auth options parser and passwd override") {
    val nebulaOptions = new NebulaOptions(
      CaseInsensitiveMap(
        Map(
          NebulaOptions.OPERATE_TYPE -> "read",
          NebulaOptions.TYPE -> "node",
          NebulaOptions.GRAPH_ADDRESS -> "127.0.0.1:9669",
          NebulaOptions.GRAPH_NAME -> "test",
          NebulaOptions.LABEL -> "person",
          NebulaOptions.USER_NAME -> "root",
          NebulaOptions.PASSWD -> "override",
          NebulaOptions.AUTHOPTIONS -> "{\"password\":\"raw\",\"retry\":3,\"enabled\":true,\"note\":\"a\\nline\"}"
        )))

    assert(nebulaOptions.authOptions.get("password") == "override")
    assert(nebulaOptions.authOptions.get("retry") == Int.box(3))
    assert(nebulaOptions.authOptions.get("enabled") == Boolean.box(true))
    assert(nebulaOptions.authOptions.get("note") == "a\nline")
  }

  test("test invalid auth options json") {
    assertThrows[IllegalArgumentException](AuthOptionsJson.parseObject("not-json"))
  }

  test("test wrong connection config") {
    assertThrows[AssertionError](NebulaConnectionConfig.builder().withTimeoutSec(1).build())
    assertThrows[AssertionError](NebulaConnectionConfig.builder().withTimeoutSec(-1).build())
    assertThrows[AssertionError](
      NebulaConnectionConfig
        .builder()
        .withGraphAddress("127.0.0.1:9669")
        .withUser("")
        .withPasswd("nebula")
        .build())
    assertThrows[AssertionError](
      NebulaConnectionConfig
        .builder()
        .withGraphAddress("127.0.0.1:9669")
        .withUser("root")
        .withPasswd("")
        .build())
    assertThrows[AssertionError](
      NebulaConnectionConfig
        .builder()
        .withGraphAddress("")
        .withUser("root")
        .withPasswd("nebula")
        .build())
  }

  test("test WriteNebulaConfig") {
    val writeNebulaConfig: WriteNebulaNodeConfig = WriteNebulaNodeConfig
      .builder()
      .withGraphName("test")
      .withNodeType("tag")
      .build()

    assert(writeNebulaConfig.getGraphName.equals("test"))
  }

  test("test wrong batch") {
    assertThrows[AssertionError](
      WriteNebulaNodeConfig
        .builder()
        .withGraphName("test")
        .withNodeType("tag")
        .withBatchSize(-1)
        .build())
  }

  test("test ReadNebulaConfig") {
    val readNebulaConfig = ReadNebulaConfig
      .builder()
      .withGraphName("test")
      .withTypeName("person")
      .withReturnCols(List("name"))
      .withBatchSize(1000)
      .withPartitionNum(1)
      .build()
    assert(readNebulaConfig.getReturnCols.size == 1)
    assert(readNebulaConfig.getGraphName.equals("test"))
    assert(readNebulaConfig.getTypeName.equals("person"))
    assert(readNebulaConfig.getBatchSize == 1000)
    assert(readNebulaConfig.getPartitionNum == 1)
  }

  test("test default ReadNebulaConfig") {
    val readNebulaConfig = ReadNebulaConfig
      .builder()
      .withGraphName("test")
      .withTypeName("person")
      .build()
    assert(readNebulaConfig.getPartitionNum == 10)
    assert(readNebulaConfig.getBatchSize == 2000)
    assert(readNebulaConfig.getReturnCols == null)
  }

  test("test wrong batchSize") {
    assertThrows[AssertionError](
      ReadNebulaConfig
        .builder()
        .withGraphName("test")
        .withTypeName("person")
        .withBatchSize(-1)
        .build()
    )

    assertThrows[AssertionError](
      ReadNebulaConfig
        .builder()
        .build()
    )
  }

}
