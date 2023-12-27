/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

package com.vesoft.nebula.connector.nebula

import com.vesoft.nebula.PropertyType
import com.vesoft.nebula.connector.mock.NebulaGraphMock
import com.vesoft.nebula.connector.{Address, DataTypeEnum}
import com.vesoft.nebula.meta.Schema
import org.apache.log4j.BasicConfigurator
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

class MetaProviderTest extends AnyFunSuite with BeforeAndAfterAll {
  BasicConfigurator.configure()
  var metaProvider: MetaProvider = null

  override def beforeAll(): Unit = {
    val addresses: List[Address] = List(new Address("127.0.0.1", 9559))
    metaProvider = new MetaProvider(addresses, 6000, 3, 3, false, null, null, null, "test")

    val graphMock = new NebulaGraphMock
    graphMock.mockStringIdGraph()
    graphMock.mockIntIdGraph()
    graphMock.close()
  }

  override def afterAll(): Unit = {
    metaProvider.close()
  }

  test("getPartitionNumber") {
    assert(metaProvider.getPartitionNumber("test_int") == 10)
    assert(metaProvider.getPartitionNumber("test_string") == 10)
  }

  test("getVidType") {
    assert(metaProvider.getVidType("test_int") == VidType.INT)
    assert(metaProvider.getVidType("test_string") == VidType.STRING)
  }

  test("getTag") {
    val schema: Schema = metaProvider.getTag("test_int", "person")
    assert(schema.columns.size() == 13)

    val schema1: Schema = metaProvider.getTag("test_string", "person")
    assert(schema1.columns.size() == 13)
  }

  test("getEdge") {
    val schema: Schema = metaProvider.getEdge("test_int", "friend")
    assert(schema.columns.size() == 13)

    val schema1: Schema = metaProvider.getEdge("test_string", "friend")
    assert(schema1.columns.size() == 13)
  }

  test("getTagSchema for person") {
    val schemaMap: Map[String, Integer] = metaProvider.getTagSchema("test_int", "person")
    assert(schemaMap.size == 13)
    assert(schemaMap("col1") == PropertyType.STRING.getValue)
    assert(schemaMap("col2") == PropertyType.FIXED_STRING.getValue)
    assert(schemaMap("col3") == PropertyType.INT8.getValue)
    assert(schemaMap("col4") == PropertyType.INT16.getValue)
    assert(schemaMap("col5") == PropertyType.INT32.getValue)
    assert(schemaMap("col6") == PropertyType.INT64.getValue)
    assert(schemaMap("col7") == PropertyType.DATE.getValue)
    assert(schemaMap("col8") == PropertyType.DATETIME.getValue)
    assert(schemaMap("col9") == PropertyType.TIMESTAMP.getValue)
    assert(schemaMap("col10") == PropertyType.BOOL.getValue)
    assert(schemaMap("col11") == PropertyType.DOUBLE.getValue)
    assert(schemaMap("col12") == PropertyType.FLOAT.getValue)
    assert(schemaMap("col13") == PropertyType.TIME.getValue)
  }

  test("getTagSchema for geo_shape") {
    val schemaMap: Map[String, Integer] = metaProvider.getTagSchema("test_int", "geo_shape")
    assert(schemaMap.size == 1)
    assert(schemaMap("geo") == PropertyType.GEOGRAPHY.getValue)
  }

  test("getEdgeSchema") {
    val schemaMap: Map[String, Integer] = metaProvider.getEdgeSchema("test_int", "friend")
    assert(schemaMap.size == 13)
    assert(schemaMap("col1") == PropertyType.STRING.getValue)
    assert(schemaMap("col2") == PropertyType.FIXED_STRING.getValue)
    assert(schemaMap("col3") == PropertyType.INT8.getValue)
    assert(schemaMap("col4") == PropertyType.INT16.getValue)
    assert(schemaMap("col5") == PropertyType.INT32.getValue)
    assert(schemaMap("col6") == PropertyType.INT64.getValue)
    assert(schemaMap("col7") == PropertyType.DATE.getValue)
    assert(schemaMap("col8") == PropertyType.DATETIME.getValue)
    assert(schemaMap("col9") == PropertyType.TIMESTAMP.getValue)
    assert(schemaMap("col10") == PropertyType.BOOL.getValue)
    assert(schemaMap("col11") == PropertyType.DOUBLE.getValue)
    assert(schemaMap("col12") == PropertyType.FLOAT.getValue)
    assert(schemaMap("col13") == PropertyType.TIME.getValue)
  }

  test("getLabelType") {
    assert(metaProvider.getLabelType("test_int", "person") == DataTypeEnum.VERTEX)
    assert(metaProvider.getLabelType("test_int", "friend") == DataTypeEnum.EDGE)
  }
}
