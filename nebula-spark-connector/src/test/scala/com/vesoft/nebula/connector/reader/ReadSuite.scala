/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

package com.vesoft.nebula.connector.reader

import com.facebook.thrift.protocol.TCompactProtocol
import com.vesoft.nebula.connector.connector.NebulaDataFrameReader
import com.vesoft.nebula.connector.{NebulaConnectionConfig, ReadNebulaConfig}
import com.vesoft.nebula.connector.mock.NebulaGraphMock
import org.apache.log4j.BasicConfigurator
import org.apache.spark.SparkConf
import org.apache.spark.sql.{Encoders, SparkSession}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

class ReadSuite extends AnyFunSuite with BeforeAndAfterAll {
  BasicConfigurator.configure()
  var sparkSession: SparkSession = null

  override def beforeAll(): Unit = {
    val graphMock = new NebulaGraphMock
    graphMock.mockIntIdGraph()
    graphMock.close()
    val sparkConf = new SparkConf
    sparkConf
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .registerKryoClasses(Array[Class[_]](classOf[TCompactProtocol]))
    sparkSession = SparkSession
      .builder()
      .master("local")
      .config(sparkConf)
      .getOrCreate()
  }

  override def afterAll(): Unit = {
    sparkSession.stop()
  }

  test("read vertex with no properties") {
    val config =
      NebulaConnectionConfig
        .builder()
        .withMetaAddress("127.0.0.1:9559")
        .withConenctionRetry(2)
        .build()
    val nebulaReadVertexConfig: ReadNebulaConfig = ReadNebulaConfig
      .builder()
      .withSpace("test_int")
      .withLabel("person")
      .withNoColumn(true)
      .withLimit(10)
      .withPartitionNum(10)
      .build()
    val vertex = sparkSession.read.nebula(config, nebulaReadVertexConfig).loadVerticesToDF()
    vertex.printSchema()
    vertex.show(truncate = false)
    assert(vertex.count() == 18)
    assert(vertex.schema.fields.length == 1)
  }

  test("read vertex with specific properties") {
    val config =
      NebulaConnectionConfig
        .builder()
        .withMetaAddress("127.0.0.1:9559")
        .withConenctionRetry(2)
        .build()
    val nebulaReadVertexConfig: ReadNebulaConfig = ReadNebulaConfig
      .builder()
      .withSpace("test_int")
      .withLabel("person")
      .withNoColumn(false)
      .withReturnCols(List("col1"))
      .withLimit(10)
      .withPartitionNum(10)
      .build()
    val vertex = sparkSession.read.nebula(config, nebulaReadVertexConfig).loadVerticesToDF()
    vertex.printSchema()
    vertex.show(truncate = false)
    assert(vertex.count() == 18)
    assert(vertex.schema.fields.length == 2)

    vertex.map(row => {
      row.getAs[Long]("_vertexId") match {
        case 1L => {
          assert(row.getAs[String]("col1").equals("person1"))
        }
        case 0L => {
          assert(row.isNullAt(1))
        }
      }
      ""
    })(Encoders.STRING)

  }

  test("read vertex with all properties") {
    val config =
      NebulaConnectionConfig
        .builder()
        .withMetaAddress("127.0.0.1:9559")
        .withConenctionRetry(2)
        .build()
    val nebulaReadVertexConfig: ReadNebulaConfig = ReadNebulaConfig
      .builder()
      .withSpace("test_int")
      .withLabel("person")
      .withNoColumn(false)
      .withReturnCols(List())
      .withLimit(10)
      .withPartitionNum(10)
      .build()
    val vertex = sparkSession.read.nebula(config, nebulaReadVertexConfig).loadVerticesToDF()
    vertex.printSchema()
    vertex.show(truncate = false)
    assert(vertex.count() == 18)
    assert(vertex.schema.fields.length == 14)

    vertex.map(row => {
      row.getAs[Long]("_vertexId") match {
        case 1L => {
          assert(row.getAs[String]("col1").equals("person1"))
          assert(row.getAs[String]("col2").equals("person1"))
          assert(row.getAs[Long]("col3") == 11)
          assert(row.getAs[Long]("col4") == 200)
          assert(row.getAs[Long]("col5") == 1000)
          assert(row.getAs[Long]("col6") == 188888)
          assert(row.getAs[String]("col7").equals("2021-01-01"))
          assert(row.getAs[String]("col8").equals("2021-01-01T12:00:00.000"))
          assert(row.getAs[Long]("col9") == 1609502400)
          assert(row.getAs[Boolean]("col10"))
          assert(row.getAs[Double]("col11") < 1.001)
          assert(row.getAs[Double]("col12") < 2.001)
          assert(row.getAs[String]("col13").equals("12:01:01"))
        }
        case 0L => {
          assert(row.isNullAt(1))
          assert(row.isNullAt(2))
          assert(row.isNullAt(3))
          assert(row.isNullAt(4))
          assert(row.isNullAt(5))
          assert(row.isNullAt(6))
          assert(row.isNullAt(7))
          assert(row.isNullAt(8))
          assert(row.isNullAt(9))
          assert(row.isNullAt(10))
          assert(row.isNullAt(11))
          assert(row.isNullAt(12))
          assert(row.isNullAt(13))
        }
      }
      ""
    })(Encoders.STRING)
  }

  test("read vertex for geo_shape") {
    val config =
      NebulaConnectionConfig
        .builder()
        .withMetaAddress("127.0.0.1:9559")
        .withConenctionRetry(2)
        .build()
    val nebulaReadVertexConfig: ReadNebulaConfig = ReadNebulaConfig
      .builder()
      .withSpace("test_int")
      .withLabel("geo_shape")
      .withNoColumn(false)
      .withReturnCols(List("geo"))
      .withLimit(10)
      .withPartitionNum(10)
      .build()
    val vertex = sparkSession.read.nebula(config, nebulaReadVertexConfig).loadVerticesToDF()
    vertex.printSchema()
    vertex.show(truncate = false)
    assert(vertex.count() == 3)
    assert(vertex.schema.fields.length == 2)

    vertex.map(row => {
      row.getAs[Long]("_vertexId") match {
        case 100L => {
          assert(row.getAs[String]("geo").equals("POINT(1 2)"))
        }
        case 101L => {
          assert(row.getAs[String]("geo").equals("LINESTRING(1 2, 3 4)"))
        }
        case 102L => {
          assert(row.getAs[String]("geo").equals("POLYGON((0 1, 1 2, 2 3, 0 1))"))
        }
      }
      ""
    })(Encoders.STRING)
  }

  test("read vertex for tag_duration") {
    val config =
      NebulaConnectionConfig
        .builder()
        .withMetaAddress("127.0.0.1:9559")
        .withConenctionRetry(2)
        .build()
    val nebulaReadVertexConfig: ReadNebulaConfig = ReadNebulaConfig
      .builder()
      .withSpace("test_int")
      .withLabel("tag_duration")
      .withNoColumn(false)
      .withReturnCols(List("col"))
      .withLimit(10)
      .withPartitionNum(10)
      .build()
    val vertex = sparkSession.read.nebula(config, nebulaReadVertexConfig).loadVerticesToDF()
    vertex.printSchema()
    vertex.show(truncate = false)
    assert(vertex.count() == 1)
    assert(vertex.schema.fields.length == 2)

    vertex.map(row => {
      row.getAs[Long]("_vertexId") match {
        case 200L => {
          assert(
            row.getAs[String]("col").equals("duration({months:1, seconds:100, microseconds:20})"))
        }
      }
      ""
    })(Encoders.STRING)
  }

  test("read edge with no properties") {
    val config =
      NebulaConnectionConfig
        .builder()
        .withMetaAddress("127.0.0.1:9559")
        .withConenctionRetry(2)
        .build()
    val nebulaReadEdgeConfig: ReadNebulaConfig = ReadNebulaConfig
      .builder()
      .withSpace("test_int")
      .withLabel("friend")
      .withNoColumn(true)
      .withLimit(10)
      .withPartitionNum(10)
      .build()
    val edge = sparkSession.read.nebula(config, nebulaReadEdgeConfig).loadEdgesToDF()
    edge.printSchema()
    edge.show(truncate = false)
    assert(edge.count() == 12)
    assert(edge.schema.fields.length == 3)

    edge.map(row => {
      row.getAs[Long]("_srcId") match {
        case 1L => {
          assert(row.getAs[Long]("_dstId") == 2)
          assert(row.getAs[Long]("_rank") == 0)
        }
      }
      ""
    })(Encoders.STRING)
  }

  test("read edge with specific properties") {
    val config =
      NebulaConnectionConfig
        .builder()
        .withMetaAddress("127.0.0.1:9559")
        .withConenctionRetry(2)
        .build()
    val nebulaReadEdgeConfig: ReadNebulaConfig = ReadNebulaConfig
      .builder()
      .withSpace("test_int")
      .withLabel("friend")
      .withNoColumn(false)
      .withReturnCols(List("col1"))
      .withLimit(10)
      .withPartitionNum(10)
      .build()
    val edge = sparkSession.read.nebula(config, nebulaReadEdgeConfig).loadEdgesToDF()
    edge.printSchema()
    edge.show(truncate = false)
    assert(edge.count() == 12)
    assert(edge.schema.fields.length == 4)
    edge.map(row => {
      row.getAs[Long]("_srcId") match {
        case 1L => {
          assert(row.getAs[Long]("_dstId") == 2)
          assert(row.getAs[Long]("_rank") == 0)
          assert(row.getAs[String]("col1").equals("friend1"))
        }
      }
      ""
    })(Encoders.STRING)
  }

  test("read edge with all properties") {
    val config =
      NebulaConnectionConfig
        .builder()
        .withMetaAddress("127.0.0.1:9559")
        .withConenctionRetry(2)
        .build()
    val nebulaReadVertexConfig: ReadNebulaConfig = ReadNebulaConfig
      .builder()
      .withSpace("test_int")
      .withLabel("friend")
      .withNoColumn(false)
      .withReturnCols(List())
      .withLimit(10)
      .withPartitionNum(10)
      .build()
    val edge = sparkSession.read.nebula(config, nebulaReadVertexConfig).loadEdgesToDF()
    edge.printSchema()
    edge.show(truncate = false)
    assert(edge.count() == 12)
    assert(edge.schema.fields.length == 16)

    edge.map(row => {
      row.getAs[Long]("_srcId") match {
        case 1L => {
          assert(row.getAs[Long]("_dstId") == 2)
          assert(row.getAs[Long]("_rank") == 0)
          assert(row.getAs[String]("col1").equals("friend1"))
          assert(row.getAs[String]("col2").equals("friend2"))
          assert(row.getAs[Long]("col3") == 11)
          assert(row.getAs[Long]("col4") == 200)
          assert(row.getAs[Long]("col5") == 1000)
          assert(row.getAs[Long]("col6") == 188888)
          assert(row.getAs[String]("col7").equals("2021-01-01"))
          assert(row.getAs[String]("col8").equals("2021-01-01T12:00:00.000"))
          assert(row.getAs[Long]("col9") == 1609502400)
          assert(row.getAs[Boolean]("col10"))
          assert(row.getAs[Double]("col11") < 1.001)
          assert(row.getAs[Double]("col12") < 2.001)
          assert(row.getAs[String]("col13").equals("12:01:01"))
        }
      }
      ""
    })(Encoders.STRING)
  }

  test("read edge from nGQL: MATCH ()-[e:friend]->() RETURN e LIMIT 1000")
  {
    val config =
      NebulaConnectionConfig
        .builder()
        .withMetaAddress("127.0.0.1:9559")
        .withGraphAddress("127.0.0.1:9669")
        .withConenctionRetry(2)
        .build()
    val nebulaReadConfig: ReadNebulaConfig = ReadNebulaConfig
      .builder()
      .withSpace("test_int")
      .withLabel("friend")
      .withNoColumn(false)
      .withLabel("friend")
      .withReturnCols(List("col1"))
      .withNgql("match ()-[e:friend]-() return e LIMIT 1000")
      .build()
    val edge = sparkSession.read.nebula(config, nebulaReadConfig).loadEdgesToDfByNgql()
    edge.printSchema()
    edge.show(truncate = false)
    assert(edge.count() == 12)
    assert(edge.schema.fields.length == 4)
    edge.map(row => {
      row.getAs[Long]("_srcId") match {
        case 1L => {
          assert(row.getAs[Long]("_dstId") == 2)
          assert(row.getAs[Long]("_rank") == 0)
          assert(row.getAs[String]("col1").equals("friend1"))
        }
      }
      ""
    })(Encoders.STRING)
  }

}
