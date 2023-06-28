/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

package com.vesoft.nebula.connector.writer

import com.vesoft.nebula.client.graph.data.ResultSet
import com.vesoft.nebula.connector.connector.Address
import com.vesoft.nebula.connector.mock.{NebulaGraphMock, SparkMock}
import com.vesoft.nebula.connector.nebula.GraphProvider
import org.apache.log4j.BasicConfigurator
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

class WriteDeleteSuite extends AnyFunSuite with BeforeAndAfterAll {
  BasicConfigurator.configure()

  override def beforeAll(): Unit = {
    val graphMock = new NebulaGraphMock
    graphMock.mockStringIdGraphSchema()
    graphMock.mockIntIdGraphSchema()
    graphMock.close()
    SparkMock.writeVertex()
  }

  test("write vertex into test_write_string space with delete mode") {
    SparkMock.deleteVertex()
    val addresses: List[Address] = List(new Address("127.0.0.1", 9669))
    val graphProvider = new GraphProvider(addresses, "root", "nebula", 3000)

    graphProvider.switchSpace("test_write_string")
    val resultSet: ResultSet =
      graphProvider.submit("use test_write_string;"
        + "match (v:person_connector) return v limit 100000;")
    assert(resultSet.isSucceeded)
    assert(resultSet.getColumnNames.size() == 1)
    assert(resultSet.isEmpty)
  }

  test("write edge into test_write_string space with delete mode") {
    SparkMock.deleteEdge()
    val addresses: List[Address] = List(new Address("127.0.0.1", "root", "nebula", 9669))
    val graphProvider = new GraphProvider(addresses, 3000)

    graphProvider.switchSpace("test_write_string")
    val resultSet: ResultSet =
      graphProvider.submit("use test_write_string;"
        + "fetch prop on friend_connector \"1\"->\"2\"@10 yield edge as e;")
    assert(resultSet.isSucceeded)
    assert(resultSet.getColumnNames.size() == 1)
    assert(resultSet.isEmpty)
  }
}
