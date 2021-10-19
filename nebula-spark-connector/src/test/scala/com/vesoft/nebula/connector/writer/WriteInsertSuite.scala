/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

package com.vesoft.nebula.connector.writer

import com.vesoft.nebula.client.graph.data.ResultSet
import com.vesoft.nebula.connector.connector.{Address}
import com.vesoft.nebula.connector.mock.{NebulaGraphMock, SparkMock}
import com.vesoft.nebula.connector.nebula.GraphProvider
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

class WriteInsertSuite extends AnyFunSuite with BeforeAndAfterAll {

  override def beforeAll(): Unit = {
    val graphMock = new NebulaGraphMock
    graphMock.mockStringIdGraphSchema()
    graphMock.mockIntIdGraphSchema()
    graphMock.close()
  }

  test("write vertex into test_write_string space with insert mode") {
    SparkMock.writeVertex()
    val addresses: List[Address] = List(new Address("127.0.0.1", 9669))
    val graphProvider            = new GraphProvider(addresses)

    graphProvider.switchSpace("root", "nebula", "test_write_string")
    val createIndexResult: ResultSet = graphProvider.submit(
      "use test_write_string; "
        + "create tag index if not exists person_index on person_connector(col1(20));")
    Thread.sleep(5000)
    graphProvider.submit("rebuild tag index person_index;")

    Thread.sleep(5000)

    val resultSet: ResultSet =
      graphProvider.submit("use test_write_string;match (v:person_connector) return v;")
    assert(resultSet.getColumnNames.size() == 1)
    assert(resultSet.getRows.size() == 13)

    for (i <- 0 until resultSet.getRows.size) {
      println(resultSet.rowValues(i).toString)
    }
  }

  test("write edge into test_write_string space with insert mode") {
    SparkMock.writeEdge()

    val addresses: List[Address] = List(new Address("127.0.0.1", 9669))
    val graphProvider            = new GraphProvider(addresses)

    graphProvider.switchSpace("root", "nebula", "test_write_string")
    val createIndexResult: ResultSet = graphProvider.submit(
      "use test_write_string; "
        + "create edge index if not exists friend_index on friend_connector(col1(20));")
    Thread.sleep(5000)
    graphProvider.submit("rebuild edge index friend_index;")

    Thread.sleep(5000)

    val resultSet: ResultSet =
      graphProvider.submit(
        "use test_write_string;match (v:person_connector)-[e:friend_connector] -> ()  return e;")
    assert(resultSet.getColumnNames.size() == 1)
    assert(resultSet.getRows.size() == 13)

    for (i <- 0 until resultSet.getRows.size) {
      println(resultSet.rowValues(i).toString)
    }
  }
}
