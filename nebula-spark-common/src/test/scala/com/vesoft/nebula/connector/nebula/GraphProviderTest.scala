/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

package com.vesoft.nebula.connector.nebula

import com.vesoft.nebula.connector.Address
import com.vesoft.nebula.connector.mock.NebulaGraphMock
import org.apache.log4j.BasicConfigurator
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

class GraphProviderTest extends AnyFunSuite with BeforeAndAfterAll {
  BasicConfigurator.configure()

  var graphProvider: GraphProvider = null

  override def beforeAll(): Unit = {
    val addresses: List[Address] = List(new Address("127.0.0.1", 9669))
    graphProvider = new GraphProvider(addresses, "root", "nebula", 3000)
    val graphMock = new NebulaGraphMock
    graphMock.mockIntIdGraph()
    graphMock.mockStringIdGraph()
    graphMock.close()
  }

  override def afterAll(): Unit = {
    graphProvider.close()
  }

  test("setHandshakeKey") {
    val addresses: List[Address] = List(new Address("127.0.0.1", 9669))
    graphProvider =
      new GraphProvider(addresses, "root", "nebula", 3000, false, null, null, null, "test")

    assert(graphProvider.switchSpace("test_int"))
  }

  test("switchSpace") {
    assertThrows[RuntimeException](graphProvider.switchSpace("space_not_exist"))
    assert(graphProvider.switchSpace("test_int"))
  }

  test("submit") {
    val result = graphProvider.submit("fetch prop on person 1 yield vertex as v")
    assert(result.isSucceeded)
  }
}
