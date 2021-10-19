/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

package com.vesoft.nebula.connector.writer

import com.facebook.thrift.protocol.TCompactProtocol
import com.vesoft.nebula.client.graph.data.ResultSet
import com.vesoft.nebula.connector.connector.{Address, NebulaDataFrameWriter}
import com.vesoft.nebula.connector.{
  NebulaConnectionConfig,
  WriteNebulaEdgeConfig,
  WriteNebulaVertexConfig
}
import com.vesoft.nebula.connector.mock.NebulaGraphMock
import com.vesoft.nebula.connector.nebula.GraphProvider
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

import scala.collection.mutable.ListBuffer

class WriteSuite extends AnyFunSuite with BeforeAndAfterAll {

  override def beforeAll(): Unit = {
    val graphMock = new NebulaGraphMock
    graphMock.mockStringIdGraphSchema()
    graphMock.mockIntIdGraphSchema()
    graphMock.close()
  }

  test("write vertex into test_write_string space with insert mode") {
    val sparkConf = new SparkConf
    sparkConf
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .registerKryoClasses(Array[Class[_]](classOf[TCompactProtocol]))
    val spark = SparkSession
      .builder()
      .master("local")
      .config(sparkConf)
      .getOrCreate()

    val df = spark.read.option("header", true).csv("src/test/resources/vertex.csv")

    val config =
      NebulaConnectionConfig
        .builder()
        .withMetaAddress("127.0.0.1:9559")
        .withGraphAddress("127.0.0.1:9669")
        .withConenctionRetry(2)
        .build()
    val nebulaWriteVertexConfig: WriteNebulaVertexConfig = WriteNebulaVertexConfig
      .builder()
      .withSpace("test_write_string")
      .withTag("person")
      .withVidField("id")
      .withVidAsProp(false)
      .withBatch(5)
      .build()
    df.write.nebula(config, nebulaWriteVertexConfig).writeVertices()

    spark.stop()

    val addresses: List[Address] = List(new Address("127.0.0.1", 9559))
    val graphProvider            = new GraphProvider(addresses)

    val createIndexResult: ResultSet = graphProvider.submit(
      "use test_write_string; "
        + "create tag index if not exists person_index on person(col1(20));")
    Thread.sleep(5000)
    graphProvider.submit(
      "use test_write_string; "
        + "rebuild tag index person_index;")

    Thread.sleep(5000)

    val resultSet: ResultSet =
      graphProvider.submit("use test_write_string;match (v:person) return v;")
    assert(resultSet.getColumnNames.size() == 1)
    assert(resultSet.getRows.size() == 13)
    val resultString: ListBuffer[String] = new ListBuffer[String]
    for (i <- 0 until resultSet.getRows.size) {
      resultString.append(resultSet.rowValues(i).toString)
    }

    val expectResultString: ListBuffer[String] = new ListBuffer[String]
    expectResultString.append(
      "ColumnName: [v], Values: [(2 :person_connector {col13: utc time: 11:10:10.000000, timezoneOffset: 0, col12: 2.0999999046325684, col11: 1.1, col8: utc datetime: 2021-01-02T12:10:10.000000, timezoneOffset: 0, col9: 43535232, col6: 41, col10: false, col7: 2021-01-28, col4: 21, col5: 31, col2: \"Jina\u0000\u0000\u0000\u0000\", col3: 11, col1: \"Jina\"} :person {col13: utc time: 12:12:12.000000, timezoneOffset: 0, col12: 1.0, col11: 1.2, col8: utc datetime: 2019-01-01T12:12:12.000000, timezoneOffset: 0, col9: 0, col10: true, col6: 0, col7: 2019-01-02, col4: 1112, col5: 22222, col2: \"abcdefgh\", col3: 2, col1: \"abb\"})]")
    expectResultString.append(
      "ColumnName: [v], Values: [(6 :person_connector {col13: utc time: 15:10:10.000000, timezoneOffset: 0, col12: 2.5, col11: 1.5, col8: utc datetime: 2021-01-06T12:10:10.000000, timezoneOffset: 0, col9: 0, col10: false, col6: 45, col7: 2021-02-02, col4: 25, col5: 35, col2: \"王五\u0000\u0000\", col3: 15, col1: \"王五\"} :person {col13: utc time: 12:12:12.000000, timezoneOffset: 0, col12: 1.0, col11: 1.2, col8: utc datetime: 2019-01-01T12:12:12.000000, timezoneOffset: 0, col9: 435463424, col10: true, col6: 0, col7: 2021-12-12, col4: 1111, col5: 2147483647, col2: \"abcdefgh\", col3: 6, col1: \"ab\\sf\"})]")
    expectResultString.append(
      "ColumnName: [v], Values: [(1 :person_connector {col13: utc time: 10:10:10.000000, timezoneOffset: 0, col12: 2.0, col11: 1.0, col8: utc datetime: 2021-01-01T12:10:10.000000, timezoneOffset: 0, col9: 43535232, col6: 40, col10: true, col7: 2021-01-27, col4: 20, col5: 30, col2: \"tom\u0000\u0000\u0000\u0000\u0000\", col3: 10, col1: \"Tom\"} :person {col13: utc time: 11:12:12.000000, timezoneOffset: 0, col12: 1.0, col11: 1.2, col8: utc datetime: 2019-01-01T12:12:12.000000, timezoneOffset: 0, col9: 435463424, col10: false, col6: 6412233, col7: 2019-01-01, col4: 1111, col5: 22222, col2: \"abcdefgh\", col3: 1, col1: \"aba\"})]")
    expectResultString.append(
      "ColumnName: [v], Values: [(-1 :person_connector {col13: utc time: 20:10:10.000000, timezoneOffset: 0, col12: 3.0, col11: 2.0, col8: utc datetime: 2021-02-11T12:10:10.000000, timezoneOffset: 0, col9: 43535232, col10: false, col6: 50, col7: 2021-02-07, col4: 30, col5: 40, col2: \"Jina\u0000\u0000\u0000\u0000\", col3: 20, col1: \"Jina\"} :person {col13: utc time: 12:12:12.000000, timezoneOffset: 0, col12: 1.0, col11: 1.2, col8: utc datetime: 2019-01-01T12:12:12.000000, timezoneOffset: 0, col9: 435463424, col10: false, col6: 6412233, col7: 2019-01-01, col4: 1111, col5: 22222, col2: \"abcdefgh\", col3: -1, col1: \"\"})]")
    expectResultString.append(
      "ColumnName: [v], Values: [(5 :person_connector {col13: utc time: 14:10:10.000000, timezoneOffset: 0, col12: 2.4000000953674316, col11: 1.4, col8: utc datetime: 2021-01-05T12:10:10.000000, timezoneOffset: 0, col9: 43535232, col6: 44, col10: false, col7: 2021-02-01, col4: 24, col5: 34, col2: \"李四\u0000\u0000\", col3: 14, col1: \"李四\"} :person {col13: utc time: 12:12:12.000000, timezoneOffset: 0, col12: 0.0, col11: 0.0, col8: utc datetime: 1970-01-01T00:00:01.000000, timezoneOffset: 0, col9: 435463424, col10: false, col6: 0, col7: 1970-01-01, col4: 1111, col5: 2147483647, col2: \"abcdefgh\", col3: 5, col1: \"a\\\"be\"})]")
    expectResultString.append(
      "ColumnName: [v], Values: [(-3 :person_connector {col13: utc time: 22:10:10.000000, timezoneOffset: 0, col12: 3.200000047683716, col11: 2.2, col8: utc datetime: 2021-04-13T12:10:10.000000, timezoneOffset: 0, col9: 43535232, col10: false, col6: 52, col7: 2021-02-09, col4: 32, col5: 42, col2: \"Jina\u0000\u0000\u0000\u0000\", col3: 22, col1: \"Jina\"} :person {col13: utc time: 12:12:12.000000, timezoneOffset: 0, col12: 10.0, col11: 10.0, col8: utc datetime: 2019-01-01T12:12:12.000000, timezoneOffset: 0, col9: 435463424, col10: false, col6: 6412233, col7: 2019-01-01, col4: 1111, col5: 22222, col2: \"\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\", col3: -3, col1: \"abm\"})]")
    expectResultString.append(
      "ColumnName: [v], Values: [(3 :person_connector {col13: utc time: 12:10:10.000000, timezoneOffset: 0, col12: 2.200000047683716, col11: 1.2, col8: utc datetime: 2021-01-03T12:10:10.000000, timezoneOffset: 0, col9: 43535232, col6: 42, col10: false, col7: 2021-01-29, col4: 22, col5: 32, col2: \"Tim\u0000\u0000\u0000\u0000\u0000\", col3: 12, col1: \"Tim\"} :person {col13: utc time: 12:12:12.000000, timezoneOffset: 0, col12: 1.0, col11: 1.2, col8: utc datetime: 2019-01-01T12:12:12.000000, timezoneOffset: 0, col9: 435463424, col10: true, col6: 9223372036854775807, col7: 2019-01-03, col4: 1111, col5: 22222, col2: \"abcdefgh\", col3: 3, col1: \"ab\\tc\"})]")
    expectResultString.append(
      "ColumnName: [v], Values: [(-2 :person_connector {col13: utc time: 21:10:10.000000, timezoneOffset: 0, col12: 3.0999999046325684, col11: 2.1, col8: utc datetime: 2021-03-12T12:10:10.000000, timezoneOffset: 0, col9: 43535232, col10: false, col6: 51, col7: 2021-02-08, col4: 31, col5: 41, col2: \"Jina\u0000\u0000\u0000\u0000\", col3: 21, col1: \"Jina\"} :person {col13: utc time: 12:12:12.000000, timezoneOffset: 0, col12: 1.0, col11: 1.2, col8: utc datetime: 2019-01-01T12:12:12.000000, timezoneOffset: 0, col9: 435463424, col6: 6412233, col10: false, col7: 2019-01-01, col4: 1111, col5: 22222, col2: NULL, col3: -2, col1: NULL})]")
    expectResultString.append(
      "ColumnName: [v], Values: [(4 :person_connector {col13: utc time: 13:10:10.000000, timezoneOffset: 0, col12: 2.299999952316284, col11: 1.3, col8: utc datetime: 2021-01-04T12:10:10.000000, timezoneOffset: 0, col9: 43535232, col6: 43, col10: true, col7: 2021-01-30, col4: 23, col5: 33, col2: \"张三\u0000\u0000\", col3: 13, col1: \"张三\"} :person {col13: utc time: 12:12:12.000000, timezoneOffset: 0, col12: 1.0, col11: 1.2, col8: utc datetime: 2019-01-01T12:12:12.000000, timezoneOffset: 0, col9: 435463424, col6: -9223372036854775808, col10: false, col7: 2019-01-04, col4: 1111, col5: 22222, col2: \"abcdefgh\", col3: 4, col1: \"a\\tbd\"})]")
    expectResultString.append(
      "ColumnName: [v], Values: [(7 :person_connector {col13: utc time: 16:10:10.000000, timezoneOffset: 0, col12: 2.5999999046325684, col11: 1.6, col8: utc datetime: 2021-01-07T12:10:10.000000, timezoneOffset: 0, col9: 43535232, col10: true, col6: 46, col7: 2021-02-03, col4: 26, col5: 36, col2: \"Jina\u0000\u0000\u0000\u0000\", col3: 16, col1: \"Jina\"} :person {col13: utc time: 12:12:12.000000, timezoneOffset: 0, col12: 1.0, col11: 1.2, col8: utc datetime: 2019-01-01T12:12:12.000000, timezoneOffset: 0, col9: 435463424, col10: false, col6: 6412233, col7: 2019-01-01, col4: 1111, col5: -2147483648, col2: \"abcdefgh\", col3: 7, col1: \"abg\"})]")
    expectResultString.append(
      "ColumnName: [v], Values: [(8 :person_connector {col13: utc time: 17:10:10.000000, timezoneOffset: 0, col12: 2.700000047683716, col11: 1.7, col8: utc datetime: 2021-01-08T12:10:10.000000, timezoneOffset: 0, col9: 43535232, col10: false, col6: 47, col7: 2021-02-04, col4: 27, col5: 37, col2: \"Jina\u0000\u0000\u0000\u0000\", col3: 17, col1: \"Jina\"} :person {col13: utc time: 12:12:12.000000, timezoneOffset: 0, col12: 1.0, col11: 1.2, col8: utc datetime: 2019-01-01T12:12:12.000000, timezoneOffset: 0, col9: 435463424, col10: false, col6: 6412233, col7: 2019-01-01, col4: 32767, col5: 0, col2: \"abcdefgh\", col3: 8, col1: \"abh\"})]")
    expectResultString.append(
      "ColumnName: [v], Values: [(9 :person_connector {col13: utc time: 18:10:10.000000, timezoneOffset: 0, col12: 2.799999952316284, col11: 1.8, col8: utc datetime: 2021-01-09T12:10:10.000000, timezoneOffset: 0, col9: 43535232, col6: 48, col10: true, col7: 2021-02-05, col4: 28, col5: 38, col2: \"Jina\u0000\u0000\u0000\u0000\", col3: 18, col1: \"Jina\"} :person {col13: utc time: 12:12:12.000000, timezoneOffset: 0, col12: 1.0, col11: 1.2, col8: utc datetime: 2019-01-01T12:12:12.000000, timezoneOffset: 0, col9: 435463424, col6: 6412233, col10: false, col7: 2019-01-01, col4: -32767, col5: -32767, col2: \"abcdefgh\", col3: 9, col1: \"abi\"})]")
    expectResultString.append(
      "ColumnName: [v], Values: [(10 :person_connector {col13: utc time: 19:10:10.000000, timezoneOffset: 0, col12: 2.9000000953674316, col11: 1.9, col8: utc datetime: 2021-01-10T12:10:10.000000, timezoneOffset: 0, col9: 43535232, col6: 49, col10: false, col7: 2021-02-06, col4: 29, col5: 39, col2: \"Jina\u0000\u0000\u0000\u0000\", col3: 19, col1: \"Jina\"} :person {col13: utc time: 12:12:12.000000, timezoneOffset: 0, col12: 1.0, col11: 1.2, col8: utc datetime: 2019-01-01T12:12:12.000000, timezoneOffset: 0, col9: 435463424, col10: false, col6: 6412233, col7: 2019-01-01, col4: -32768, col5: 32767, col2: \"abcdefgh\", col3: 10, col1: \"abj\"})]")

    assert(resultString.containsSlice(expectResultString))
  }

  test("write edge into test_write_string space with insert mode") {
    val sparkConf = new SparkConf
    sparkConf
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .registerKryoClasses(Array[Class[_]](classOf[TCompactProtocol]))
    val spark = SparkSession
      .builder()
      .master("local")
      .config(sparkConf)
      .getOrCreate()

    val df = spark.read.option("header", true).csv("src/test/resources/edge.csv")

    val config =
      NebulaConnectionConfig
        .builder()
        .withMetaAddress("127.0.0.1:9559")
        .withGraphAddress("127.0.0.1:9669")
        .withConenctionRetry(2)
        .build()
    val nebulaWriteEdgeConfig: WriteNebulaEdgeConfig = WriteNebulaEdgeConfig
      .builder()
      .withSpace("test_write_string")
      .withEdge("friend")
      .withSrcIdField("id1")
      .withDstIdField("id2")
      .withRankField("col3")
      .withRankAsProperty(true)
      .withBatch(5)
      .build()
    df.write.nebula(config, nebulaWriteEdgeConfig).writeEdges()

    spark.stop()

    val addresses: List[Address] = List(new Address("127.0.0.1", 9559))
    val graphProvider            = new GraphProvider(addresses)

    val createIndexResult: ResultSet = graphProvider.submit(
      "use test_write_string; "
        + "create edge index if not exists friend_index on friend(col1(20));")
    Thread.sleep(5000)
    graphProvider.submit(
      "use test_write_string; "
        + "rebuild edge index friend_index;")

    Thread.sleep(5000)

    val resultSet: ResultSet =
      graphProvider.submit("use test_write_string;match (v:person) return v;")
    assert(resultSet.getColumnNames.size() == 1)
    assert(resultSet.getRows.size() == 13)
    val resultString: ListBuffer[String] = new ListBuffer[String]
    for (i <- 0 until resultSet.getRows.size) {
      resultString.append(resultSet.rowValues(i).toString)
    }

    val expectResultString: ListBuffer[String] = new ListBuffer[String]
    expectResultString.append(
      "ColumnName: [e], Values: [(2)-[:friend_connector@11{col13: utc time: 11:10:10.000000, timezoneOffset: 0, col12: 2.0999999046325684, col11: 1.1, col8: utc datetime: 2021-01-02T12:10:10.000000, timezoneOffset: 0, col9: 43535232, col6: 41, col10: false, col7: 2021-01-28, col4: 21, col5: 31, col2: \"Jina\u0000\u0000\u0000\u0000\", col3: 11, col1: \"Jina\"}]->(3)]")
    expectResultString.append(
      "ColumnName: [e], Values: [(6)-[:friend_connector@15{col13: utc time: 15:10:10.000000, timezoneOffset: 0, col12: 2.5, col11: 1.5, col8: utc datetime: 2021-01-06T12:10:10.000000, timezoneOffset: 0, col9: 0, col10: false, col6: 45, col7: 2021-02-02, col4: 25, col5: 35, col2: \"王五\u0000\u0000\", col3: 15, col1: \"王五\"}]->(7)]")
    expectResultString.append(
      "ColumnName: [e], Values: [(1)-[:friend_connector@10{col13: utc time: 10:10:10.000000, timezoneOffset: 0, col12: 2.0, col11: 1.0, col8: utc datetime: 2021-01-01T12:10:10.000000, timezoneOffset: 0, col9: 43535232, col6: 40, col10: true, col7: 2021-01-27, col4: 20, col5: 30, col2: \"tom\u0000\u0000\u0000\u0000\u0000\", col3: 10, col1: \"Tom\"}]->(2)]")
    expectResultString.append(
      "ColumnName: [e], Values: [(-1)-[:friend_connector@20{col13: utc time: 20:10:10.000000, timezoneOffset: 0, col12: 3.0, col11: 2.0, col8: utc datetime: 2021-02-11T12:10:10.000000, timezoneOffset: 0, col9: 43535232, col10: false, col6: 50, col7: 2021-02-07, col4: 30, col5: 40, col2: \"Jina\u0000\u0000\u0000\u0000\", col3: 20, col1: \"Jina\"}]->(5)]")
    expectResultString.append(
      "ColumnName: [e], Values: [(5)-[:friend_connector@14{col13: utc time: 14:10:10.000000, timezoneOffset: 0, col12: 2.4000000953674316, col11: 1.4, col8: utc datetime: 2021-01-05T12:10:10.000000, timezoneOffset: 0, col9: 43535232, col10: false, col6: 44, col7: 2021-02-01, col4: 24, col5: 34, col2: \"李四\u0000\u0000\", col3: 14, col1: \"李四\"}]->(6)]")
    expectResultString.append(
      "ColumnName: [e], Values: [(-3)-[:friend_connector@22{col13: utc time: 22:10:10.000000, timezoneOffset: 0, col12: 3.200000047683716, col11: 2.2, col8: utc datetime: 2021-04-13T12:10:10.000000, timezoneOffset: 0, col9: 43535232, col6: 52, col10: false, col7: 2021-02-09, col4: 32, col5: 42, col2: \"Jina\u0000\u0000\u0000\u0000\", col3: 22, col1: \"Jina\"}]->(7)]")
    expectResultString.append(
      "ColumnName: [e], Values: [(3)-[:friend_connector@12{col13: utc time: 12:10:10.000000, timezoneOffset: 0, col12: 2.200000047683716, col11: 1.2, col8: utc datetime: 2021-01-03T12:10:10.000000, timezoneOffset: 0, col9: 43535232, col6: 42, col10: false, col7: 2021-01-29, col4: 22, col5: 32, col2: \"Tim\u0000\u0000\u0000\u0000\u0000\", col3: 12, col1: \"Tim\"}]->(4)]")
    expectResultString.append(
      "ColumnName: [e], Values: [(-2)-[:friend_connector@21{col13: utc time: 21:10:10.000000, timezoneOffset: 0, col12: 3.0999999046325684, col11: 2.1, col8: utc datetime: 2021-03-12T12:10:10.000000, timezoneOffset: 0, col9: 43535232, col6: 51, col10: false, col7: 2021-02-08, col4: 31, col5: 41, col2: \"Jina\u0000\u0000\u0000\u0000\", col3: 21, col1: \"Jina\"}]->(6)]")
    expectResultString.append(
      "ColumnName: [e], Values: [(4)-[:friend_connector@13{col13: utc time: 13:10:10.000000, timezoneOffset: 0, col12: 2.299999952316284, col11: 1.3, col8: utc datetime: 2021-01-04T12:10:10.000000, timezoneOffset: 0, col9: 43535232, col10: true, col6: 43, col7: 2021-01-30, col4: 23, col5: 33, col2: \"张三\u0000\u0000\", col3: 13, col1: \"张三\"}]->(5)]")
    expectResultString.append(
      "ColumnName: [e], Values: [(7)-[:friend_connector@16{col13: utc time: 16:10:10.000000, timezoneOffset: 0, col12: 2.5999999046325684, col11: 1.6, col8: utc datetime: 2021-01-07T12:10:10.000000, timezoneOffset: 0, col9: 43535232, col6: 46, col10: true, col7: 2021-02-03, col4: 26, col5: 36, col2: \"Jina\u0000\u0000\u0000\u0000\", col3: 16, col1: \"Jina\"}]->(1)]")
    expectResultString.append(
      "ColumnName: [e], Values: [(8)-[:friend_connector@17{col13: utc time: 17:10:10.000000, timezoneOffset: 0, col12: 2.700000047683716, col11: 1.7, col8: utc datetime: 2021-01-08T12:10:10.000000, timezoneOffset: 0, col9: 43535232, col10: false, col6: 47, col7: 2021-02-04, col4: 27, col5: 37, col2: \"Jina\u0000\u0000\u0000\u0000\", col3: 17, col1: \"Jina\"}]->(1)]")
    expectResultString.append(
      "ColumnName: [e], Values: [(9)-[:friend_connector@18{col13: utc time: 18:10:10.000000, timezoneOffset: 0, col12: 2.799999952316284, col11: 1.8, col8: utc datetime: 2021-01-09T12:10:10.000000, timezoneOffset: 0, col9: 43535232, col6: 48, col10: true, col7: 2021-02-05, col4: 28, col5: 38, col2: \"Jina\u0000\u0000\u0000\u0000\", col3: 18, col1: \"Jina\"}]->(1)]")
    expectResultString.append(
      "ColumnName: [e], Values: [(10)-[:friend_connector@19{col13: utc time: 19:10:10.000000, timezoneOffset: 0, col12: 2.9000000953674316, col11: 1.9, col8: utc datetime: 2021-01-10T12:10:10.000000, timezoneOffset: 0, col9: 43535232, col6: 49, col10: false, col7: 2021-02-06, col4: 29, col5: 39, col2: \"Jina\u0000\u0000\u0000\u0000\", col3: 19, col1: \"Jina\"}]->(2)]")
    assert(resultString.containsSlice(expectResultString))
  }
}
