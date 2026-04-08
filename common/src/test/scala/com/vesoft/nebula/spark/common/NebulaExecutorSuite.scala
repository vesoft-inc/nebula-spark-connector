/*
 * Copyright (c) 2025 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

package com.vesoft.nebula.spark.common

import com.vesoft.nebula.spark.common.writer.NebulaExecutor
import org.apache.log4j.BasicConfigurator
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow
import org.apache.spark.sql.types.{BooleanType, DataTypes, LongType, StringType, StructField, StructType}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

import scala.collection.mutable.ListBuffer

class NebulaExecutorSuite extends AnyFunSuite with BeforeAndAfterAll {
  val graphName = "nba"

  BasicConfigurator.configure()
  var schema: StructType  = _
  var row   : InternalRow = _

  override def beforeAll(): Unit = {
    val fields = new ListBuffer[StructField]
    fields.append(DataTypes.createStructField("col1", StringType, false))
    fields.append(DataTypes.createStructField("col2", BooleanType, false))
    fields.append(DataTypes.createStructField("col3", LongType, false))
    schema = new StructType(fields.toArray)

    val values = new ListBuffer[Any]
    values.append("aaa")
    values.append(true)
    values.append(1L)
    row = new GenericInternalRow(values.toArray)
  }

  override def afterAll(): Unit = super.afterAll()

  test("test extraID") {
    // test string primary key
    var index         : Int     = 0
    val isPkStringType: Boolean = true
    val stringId                = NebulaExecutor.extraValue(row, schema, index, Map("col1" -> "STRING"))
    assert("\"aaa\"".equals(stringId))

    // test int primary key
    index = 2
    val hashId = NebulaExecutor.extraValue(row, schema, index, Map("col3" -> "INT64"))
    assert("1".equals(hashId))
  }

  test("test extraID with null primary key value") {
    val values = new ListBuffer[Any]
    values.append(null)
    values.append(true)
    values.append(1L)
    val rowTest: InternalRow = new GenericInternalRow(values.toArray)
    val index  : Int         = 0
    assert(NebulaExecutor.extraValue(rowTest, schema, index, Map("col1" -> "STRING")) == null)
  }

  test("test extraValue for empty value") {
    val values = new ListBuffer[Any]
    values.append("")
    values.append("")
    values.append("")
    val rowTest: InternalRow = new GenericInternalRow(values.toArray)
    assert(NebulaExecutor.extraPropValue(rowTest, schema, 0, "STRING").equals("\"\""))
    assert(NebulaExecutor.extraPropValue(rowTest, schema, 1, "INT8") == null)
    assert(NebulaExecutor.extraPropValue(rowTest, schema, 2, "BOOL") == null)
    assert(NebulaExecutor.extraPropValue(rowTest, schema, 0, "FLOAT") == null)
    assert(NebulaExecutor.extraPropValue(rowTest, schema, 1, "DOUBLE") == null)
    assert(NebulaExecutor.extraPropValue(rowTest, schema, 2, "DATE") == null)
    assert(NebulaExecutor.extraPropValue(rowTest, schema, 0, "LOCAL DATETIME") == null)
    assert(NebulaExecutor.extraPropValue(rowTest, schema, 1, "LOCAL TIME") == null)
    assert(NebulaExecutor.extraPropValue(rowTest, schema, 2, "ZONED TIME") == null)
    assert(NebulaExecutor.extraPropValue(rowTest, schema, 0, "ZONED DATETIME") == null)
  }

  test("test date type value") {
    val values = new ListBuffer[Any]
    values.append("2024-12-12")
    values.append("")
    values.append(null)
    val rowTest: InternalRow = new GenericInternalRow(values.toArray)
    assert(NebulaExecutor.extraPropValue(rowTest, schema, 0, "DATE").equals("date(\"2024-12-12\")"))
    assert(NebulaExecutor.extraPropValue(rowTest, schema, 1, "DATE") == null)
    assert(NebulaExecutor.extraPropValue(rowTest, schema, 2, "DATE") == null)
  }

  test("test local datetime type value") {
    val values = new ListBuffer[Any]
    values.append("2024-12-12T12:00:00")
    values.append("")
    values.append(null)
    val rowTest: InternalRow = new GenericInternalRow(values.toArray)
    assert(NebulaExecutor.extraPropValue(rowTest, schema, 0, "LOCAL DATETIME").equals("local_datetime(\"2024-12-12T12:00:00\")"))
    assert(NebulaExecutor.extraPropValue(rowTest, schema, 1, "LOCAL DATETIME") == null)
    assert(NebulaExecutor.extraPropValue(rowTest, schema, 2, "LOCAL DATETIME") == null)
  }

  test("test zoned datetime type value") {
    val values = new ListBuffer[Any]
    values.append("2024-12-12T12:00:00Z")
    values.append("")
    values.append(null)
    val rowTest: InternalRow = new GenericInternalRow(values.toArray)
    assert(NebulaExecutor.extraPropValue(rowTest, schema, 0, "ZONED DATETIME").equals("zoned_datetime(\"2024-12-12T12:00:00Z\")"))
    assert(NebulaExecutor.extraPropValue(rowTest, schema, 1, "ZONED DATETIME") == null)
    assert(NebulaExecutor.extraPropValue(rowTest, schema, 2, "ZONED DATETIME") == null)
  }

  test("test local time type value") {
    val values = new ListBuffer[Any]
    values.append("12:00:00")
    values.append("")
    values.append(null)
    val rowTest: InternalRow = new GenericInternalRow(values.toArray)
    assert(NebulaExecutor.extraPropValue(rowTest, schema, 0, "LOCAL TIME").equals("local_time(\"12:00:00\")"))
    assert(NebulaExecutor.extraPropValue(rowTest, schema, 1, "LOCAL TIME") == null)
    assert(NebulaExecutor.extraPropValue(rowTest, schema, 2, "LOCAL TIME") == null)
  }

  test("test zoned time type value") {
    val values = new ListBuffer[Any]
    values.append("12:00:00Z")
    values.append("")
    values.append(null)
    val rowTest: InternalRow = new GenericInternalRow(values.toArray)
    assert(NebulaExecutor.extraPropValue(rowTest, schema, 0, "ZONED TIME").equals("zoned_time(\"12:00:00Z\")"))
    assert(NebulaExecutor.extraPropValue(rowTest, schema, 1, "ZONED TIME") == null)
    assert(NebulaExecutor.extraPropValue(rowTest, schema, 2, "ZONED TIME") == null)
  }

  test("test list type value") {
    val values = new ListBuffer[Any]
    values.append("[1,2,3]")
    values.append("[a,b,c]")
    values.append("[a\n,b\",\nc']")
    val rowTest: InternalRow = new GenericInternalRow(values.toArray)
    assert(NebulaExecutor.extraPropValue(rowTest, schema, 0, "LIST<INT32>").equals("LIST[1,2,3]"))
    assert(NebulaExecutor.extraPropValue(rowTest, schema, 1, "LIST<STRING>").equals("LIST[\"a\",\"b\",\"c\"]"))
    assert(NebulaExecutor.extraPropValue(rowTest, schema, 2, "LIST<STRING>").equals("LIST[\"a\\n\",\"b\\\"\",\"\\nc\\'\"]"))
  }

  test("test vector type value") {
    val values = new ListBuffer[Any]
    values.append("[1.0,2.0,3.0]")
    values.append("")
    values.append(null)
    val rowTest: InternalRow = new GenericInternalRow(values.toArray)
    assert(NebulaExecutor.extraPropValue(rowTest, schema, 0, "VECTOR<3, FLOAT>").equals("VECTOR<3, FLOAT>([1.0,2.0,3.0])"))
    assert(NebulaExecutor.extraPropValue(rowTest, schema, 1, "VECTOR<3, FLOAT>") == null)
    assert(NebulaExecutor.extraPropValue(rowTest, schema, 2, "VECTOR<3, FLOAT>") == null)
  }

  test("test GEO type value") {
    val values = new ListBuffer[Any]
    values.append("POINT(3 8)")
    values.append("")
    values.append(null)
    val rowTest: InternalRow = new GenericInternalRow(values.toArray)
    assert(NebulaExecutor.extraPropValue(rowTest, schema, 0, "GEOGRAPHY<ANY>").equals("ST_GeogFromText(\"POINT(3 8)\")"))
    assert(NebulaExecutor.extraPropValue(rowTest, schema, 1, "GEOGRAPHY<ANY>") == null)
    assert(NebulaExecutor.extraPropValue(rowTest, schema, 2, "GEOGRAPHY<ANY>") == null)
  }

  test("test src pk & dst pk all as prop for assignEdgeValues") {
    val fieldTypeMap: Map[String, String] =
      Map("col1" -> "STRING", "col2" -> "STRING", "col3" -> "STRING")

    val prop = NebulaExecutor.assignEdgeValues(schema, row, List(0), List(1), true, true, fieldTypeMap)
    assert(prop.size == 3)
  }

  test("test src pk & dst all not as prop for assignEdgeValues") {
    val fieldTypeMap: Map[String, String] =
      Map("col1" -> "STRING", "col2" -> "STRING", "col3" -> "STRING")

    val prop =
      NebulaExecutor.assignEdgeValues(schema, row, List(0), List(1), false, false, fieldTypeMap)
    assert(prop.size == 1)
  }

  test("test toExecuteSentence for node") {
    val nodes: ListBuffer[NebulaNode] = new ListBuffer[NebulaNode]
    val nodeType                      = "person"

    val props1: Map[String, String] = Map(
      "col_string" -> "\"Tom\"",
      "col_fixed_string" -> "\"Tom\"",
      "col_bool" -> "true",
      "col_int" -> "10",
      "col_int64" -> "100",
      "col_double" -> "1.0",
      "col_date" -> "date(\"2021-11-12\")"
      )
    val props2: Map[String, String] =
      Map(
        "col_string" -> "\"Bob\"",
        "col_fixed_string" -> "\"Bob\"",
        "col_bool" -> "false",
        "col_int" -> "20",
        "col_int64" -> "200",
        "col_double" -> "2.0",
        "col_date" -> "date(\"2021-05-01\")"
        )
    nodes.append(NebulaNode(props1))
    nodes.append(NebulaNode(props2))
    val fieldTypeMap: Map[String, String] =
      Map("col_string" -> "STRING", "col_fixed_string" -> "STRING", "col_bool" -> "BOOL", "col_int" -> "INT32", "col_int64" -> "INT64", "col_double" -> "DOUBLE", "col_date" -> "DATE")
    val nebulaNodes                       = NebulaNodes(nodeType, nodes.toList, List("col_string"), fieldTypeMap)
    // test insert node
    var nodeStatement                     =
      NebulaExecutor.toInsertSentence(graphName, nebulaNodes, "")
    var expectStatement                   =
      s"""
         |TABLE t {`col_string`,`col_fixed_string`,`col_bool`,`col_int`,`col_int64`,`col_double`,`col_date`} =
         |(\"Tom\",\"Tom\",true,10,100,1.0,date(\"2021-11-12\")),(\"Bob\",\"Bob\",false,20,200,2.0,date(\"2021-05-01\"))
         |USE `$graphName`
         |FOR r IN t
         |INSERT  (@`$nodeType`{`col_string`:CAST(r.`col_string` AS STRING),`col_fixed_string`:CAST(r.`col_fixed_string` AS STRING),`col_bool`:CAST(r.`col_bool` AS BOOL),`col_int`:CAST(r.`col_int` AS INT32),`col_int64`:CAST(r.`col_int64` AS INT64),`col_double`:CAST(r.`col_double` AS DOUBLE),`col_date`:CAST(r.`col_date` AS DATE)})
         |""".stripMargin
    assert(expectStatement.toCharArray.sorted.mkString("").equals(nodeStatement.toCharArray.sorted.mkString("")))

    // test insert node with replace
    nodeStatement =
      NebulaExecutor.toInsertSentence(graphName, nebulaNodes, "OR REPLACE")
    expectStatement =
      s"""
         |TABLE t {`col_string`,`col_fixed_string`,`col_bool`,`col_int`,`col_int64`,`col_double`,`col_date`} =
         |(\"Tom\",\"Tom\",true,10,100,1.0,date(\"2021-11-12\")),(\"Bob\",\"Bob\",false,20,200,2.0,date(\"2021-05-01\"))
         |USE `$graphName`
         |FOR r IN t
         |INSERT OR REPLACE (@`$nodeType`{`col_string`:CAST(r.`col_string` AS STRING),`col_fixed_string`:CAST(r.`col_fixed_string` AS STRING),`col_bool`:CAST(r.`col_bool` AS BOOL),`col_int`:CAST(r.`col_int` AS INT32),`col_int64`:CAST(r.`col_int64` AS INT64),`col_double`:CAST(r.`col_double` AS DOUBLE),`col_date`:CAST(r.`col_date` AS DATE)})
         |""".stripMargin
    assert(expectStatement.toCharArray.sorted.mkString("").equals(nodeStatement.toCharArray.sorted.mkString("")))

    // test insert node with ignore
    nodeStatement =
      NebulaExecutor.toInsertSentence(graphName, nebulaNodes, "OR IGNORE")
    expectStatement =
      s"""
         |TABLE t {`col_string`,`col_fixed_string`,`col_bool`,`col_int`,`col_int64`,`col_double`,`col_date`} =
         |(\"Tom\",\"Tom\",true,10,100,1.0,date(\"2021-11-12\")),(\"Bob\",\"Bob\",false,20,200,2.0,date(\"2021-05-01\"))
         |USE `$graphName`
         |FOR r IN t
         |INSERT OR IGNORE (@`$nodeType`{`col_string`:CAST(r.`col_string` AS STRING),`col_fixed_string`:CAST(r.`col_fixed_string` AS STRING),`col_bool`:CAST(r.`col_bool` AS BOOL),`col_int`:CAST(r.`col_int` AS INT32),`col_int64`:CAST(r.`col_int64` AS INT64),`col_double`:CAST(r.`col_double` AS DOUBLE),`col_date`:CAST(r.`col_date` AS DATE)})
         |""".stripMargin
    assert(expectStatement.toCharArray.sorted.mkString("").equals(nodeStatement.toCharArray.sorted.mkString("")))

  }

  test("test toInsertSentence for edge") {
    val edges : ListBuffer[NebulaEdge] = new ListBuffer[NebulaEdge]
    val edgeType                       = "friend"
    val props1: Map[String, String]    = Map(
      "col_string" -> "\"Tom\"",
      "col_fixed_string" -> "\"Tom\"",
      "col_bool" -> "true",
      "col_int" -> "10",
      "col_int64" -> "100",
      "col_double" -> "1.0",
      "col_date" -> "date(\"2021-11-12\")"
      )
    val props2: Map[String, String]    =
      Map(
        "col_string" -> "\"Bob\"",
        "col_fixed_string" -> "\"Bob\"",
        "col_bool" -> "false",
        "col_int" -> "20",
        "col_int64" -> "200",
        "col_double" -> "2.0",
        "col_date" -> "date(\"2021-05-01\")"
        )
    edges.append(NebulaEdge(Map("id" -> "\"vid1\""), Map("id" -> "\"vid2\""), props1))
    edges.append(NebulaEdge(Map("id" -> "\"vid2\""), Map("id" -> "\"vid1\""), props2))

    val fieldTypeMap: Map[String, String] =
      Map("col_string" -> "STRING", "col_fixed_string" -> "STRING", "col_bool" -> "BOOL", "col_int" -> "INT32", "col_int64" -> "INT64", "col_double" -> "DOUBLE", "col_date" -> "DATE")

    val nebulaEdges: NebulaEdges = NebulaEdges(edgeType, "person", List("id"), Map("id" -> "STRING"), List("id1"), "person", List("id"), Map("id" -> "STRING"), List("id2"), List(), edges.toList, fieldTypeMap)
    val edgeStatement            = NebulaExecutor.toInsertSentence(graphName, nebulaEdges, "", true)

    val exptStatement =
      s"""
         |TABLE t {`id1`,`id2`,`col_string`,`col_fixed_string`,`col_bool`,`col_int`,`col_int64`,`col_double`,`col_date`} =
         |(\"vid1\",\"vid2\",\"Tom\",\"Tom\",true,10,100,1.0,date(\"2021-11-12\")),(\"vid2\",\"vid1\",\"Bob\",\"Bob\",false,20,200,2.0,date(\"2021-05-01\"))
         |USE `$graphName`
         |FOR r IN t
         |RETURN r.`id1` as _id1,r.`id2` as _id2,r.`col_string` as _col_string,r.`col_fixed_string` as _col_fixed_string,r.`col_bool` as _col_bool,r.`col_int` as _col_int,r.`col_int64` as _col_int64,r.`col_double` as _col_double, r.`col_date` as _col_date
         |NEXT
         |USE `$graphName`
         |OPTIONAL MATCH (src_node@`person`) WHERE src_node.`id`=CAST(_id1 AS STRING)
         |OPTIONAL MATCH (dst_node@`person`) WHERE dst_node.`id`=CAST(_id2 AS STRING)
         |INSERT  (src_node)-[e@`friend`{`col_string`:CAST(_col_string AS STRING),`col_fixed_string`:CAST(_col_fixed_string AS STRING),`col_bool`:CAST(_col_bool AS BOOL),`col_int`:CAST(_col_int AS INT32),`col_int64`:CAST(_col_int64 AS INT64),`col_double`:CAST(_col_double AS DOUBLE),`col_date`:CAST(_col_date AS DATE)}]->(dst_node)
         |""".stripMargin

    assert(exptStatement.toCharArray.sorted.mkString("").trim.equals(edgeStatement.toCharArray.sorted.mkString("").trim))
  }


  test("test toInsertSentence for edge with multiple pks") {
    val edges : ListBuffer[NebulaEdge] = new ListBuffer[NebulaEdge]
    val edgeType                       = "friend"
    val props1: Map[String, String]    = Map(
      "col_string" -> "\"Tom\"",
      "col_fixed_string" -> "\"Tom\"",
      "col_bool" -> "true",
      "col_int" -> "10",
      "col_int64" -> "100",
      "col_double" -> "1.0",
      "col_date" -> "date(\"2021-11-12\")"
      )
    val props2: Map[String, String]    =
      Map(
        "col_string" -> "\"Bob\"",
        "col_fixed_string" -> "\"Bob\"",
        "col_bool" -> "false",
        "col_int" -> "20",
        "col_int64" -> "200",
        "col_double" -> "2.0",
        "col_date" -> "date(\"2021-05-01\")"
        )
    edges.append(NebulaEdge(Map("id1" -> "\"id_1\"", "id2" -> "\"id_2\""), Map("id1" -> "\"id_3\"", "id2" -> "\"id_4\""), props1))
    edges.append(NebulaEdge(Map("id1" -> "\"id_3\"", "id2" -> "\"id_4\""), Map("id1" -> "\"id_1\"", "id2" -> "\"id_2\""), props2))

    val fieldTypeMap: Map[String, String] =
      Map("col_string" -> "STRING", "col_fixed_string" -> "STRING", "col_bool" -> "BOOL", "col_int" -> "INT32", "col_int64" -> "INT64", "col_double" -> "DOUBLE", "col_date" -> "DATE")

    val nebulaEdges: NebulaEdges = NebulaEdges(edgeType, "person",
                                               List("id1", "id2"),
                                               Map("id1" -> "STRING", "id2" -> "STRING"),
                                               List("dfId1", "dfId2"), "person",
                                               List("id1", "id2"),
                                               Map("id1" -> "STRING", "id2" -> "STRING"),
                                               List("dfId3", "dfId4"),
                                               List(),
                                               edges.toList,
                                               fieldTypeMap)

    val edgeStatement = NebulaExecutor.toInsertSentence(graphName, nebulaEdges, "", true)

    val exptStatement =
      s"""
         |TABLE t {`dfId1`,`dfId2`,`dfId3`,`dfId4`,`col_string`,`col_fixed_string`,`col_bool`,`col_int`,`col_int64`,`col_double`,`col_date`} =
         |(\"id_1\",\"id_2\",\"id_3\",\"id_4\",\"Tom\",\"Tom\",true,10,100,1.0,date(\"2021-11-12\")),(\"id_3\",\"id_4\",\"id_1\",\"id_2\",\"Bob\",\"Bob\",false,20,200,2.0,date(\"2021-05-01\"))
         |USE `$graphName`
         |FOR r IN t
         |RETURN r.`dfId1` as _dfId1,r.`dfId2` as _dfId2,r.`dfId3` as _dfId3,r.`dfId4` as _dfId4,r.`col_string` as _col_string,r.`col_fixed_string` as _col_fixed_string,r.`col_bool` as _col_bool,r.`col_int` as _col_int,r.`col_int64` as _col_int64,r.`col_double` as _col_double, r.`col_date` as _col_date
         |NEXT
         |USE `$graphName`
         |OPTIONAL MATCH (src_node@`person`) WHERE src_node.`id1`=CAST(_dfId1 AS STRING) AND src_node.`id2`=CAST(_dfId2 AS STRING)
         |OPTIONAL MATCH (dst_node@`person`) WHERE dst_node.`id1`=CAST(_dfId3 AS STRING) AND dst_node.`id2`=CAST(_dfId4 AS STRING)
         |INSERT  (src_node)-[e@`friend`{`col_string`:CAST(_col_string AS STRING),`col_fixed_string`:CAST(_col_fixed_string AS STRING),`col_bool`:CAST(_col_bool AS BOOL),`col_int`:CAST(_col_int AS INT32),`col_int64`:CAST(_col_int64 AS INT64),`col_double`:CAST(_col_double AS DOUBLE),`col_date`:CAST(_col_date AS DATE)}]->(dst_node)
         |""".stripMargin

    assert(exptStatement.toCharArray.sorted.mkString("").trim.equals(edgeStatement.toCharArray.sorted.mkString("").trim))
  }

  test("test toDeleteSentence for node") {
    val nodes: ListBuffer[NebulaNode] = new ListBuffer[NebulaNode]
    val nodeType                      = "person"

    val props1: Map[String, String] = Map(
      "col_string" -> "\"Tom\"",
      "col_fixed_string" -> "\"Tom\"",
      "col_bool" -> "true",
      "col_int" -> "10",
      "col_int64" -> "100",
      "col_double" -> "1.0",
      "col_date" -> "date(\"2021-11-12\")"
      )
    val props2: Map[String, String] =
      Map(
        "col_string" -> "\"Bob\"",
        "col_fixed_string" -> "\"Bob\"",
        "col_bool" -> "false",
        "col_int" -> "20",
        "col_int64" -> "200",
        "col_double" -> "2.0",
        "col_date" -> "date(\"2021-05-01\")"
        )
    nodes.append(NebulaNode(props1))
    nodes.append(NebulaNode(props2))

    val fieldTypeMap: Map[String, String] =
      Map("col_string" -> "STRING", "col_fixed_string" -> "STRING", "col_bool" -> "BOOL", "col_int" -> "INT32", "col_int64" -> "INT64", "col_double" -> "DOUBLE", "col_date" -> "DATE")
    val nebulaNodes                       = NebulaNodes(nodeType, nodes.toList, List("col_string"), fieldTypeMap)
    val nodeStatement                     =
      NebulaExecutor.toDeleteSentence(graphName, nodeType, nebulaNodes, "DETACH DELETE")

    val expectStatement =
      s"""USE `$graphName` MATCH (a@`$nodeType` where a.`col_string` in [\"Bob\",\"Tom\"]) DETACH DELETE a""".stripMargin

    expectStatement.toCharArray.sorted.mkString("")
    assert(expectStatement.toCharArray.sorted.mkString("").equals(nodeStatement.toCharArray.sorted.mkString("")))
  }


  test("test toDeleteSentence for node with multiple pks") {
    val nodes: ListBuffer[NebulaNode] = new ListBuffer[NebulaNode]
    val nodeType                      = "person"

    val props1: Map[String, String] = Map(
      "col_string" -> "\"Tom\"",
      "col_fixed_string" -> "\"Tom\"",
      "col_bool" -> "true",
      "col_int" -> "10",
      "col_int64" -> "100",
      "col_double" -> "1.0",
      "col_date" -> "date(\"2021-11-12\")"
      )
    val props2: Map[String, String] =
      Map(
        "col_string" -> "\"Bob\"",
        "col_fixed_string" -> "\"Bob\"",
        "col_bool" -> "false",
        "col_int" -> "20",
        "col_int64" -> "200",
        "col_double" -> "2.0",
        "col_date" -> "date(\"2021-05-01\")"
        )
    nodes.append(NebulaNode(props1))
    nodes.append(NebulaNode(props2))

    val fieldTypeMap: Map[String, String] =
      Map("col_string" -> "STRING", "col_fixed_string" -> "STRING", "col_bool" -> "BOOL", "col_int" -> "INT32", "col_int64" -> "INT64", "col_double" -> "DOUBLE", "col_date" -> "DATE")
    val nebulaNodes                       = NebulaNodes(nodeType, nodes.toList, List("col_string", "col_fixed_string"), fieldTypeMap)
    val nodeStatement                     =
      NebulaExecutor.toDeleteSentence(graphName, nodeType, nebulaNodes, "DETACH DELETE")

    val expectStatement =
      s"""
         |TABLE t {`col_string`,`col_fixed_string`,`col_bool`,`col_int`,`col_int64`,`col_double`,`col_date`} =
         |(\"Tom\",\"Tom\",true,10,100,1.0,date(\"2021-11-12\")),(\"Bob\",\"Bob\",false,20,200,2.0,date(\"2021-05-01\"))
         |USE `$graphName`
         |FOR r IN t
         |MATCH(v_node@`person`) WHERE v_node.`col_string`=CAST(r.`col_string` AS STRING) AND v_node.`col_fixed_string`=CAST(r.`col_fixed_string` AS STRING)
         |DETACH DELETE v_node
         |""".stripMargin

    assert(expectStatement.toCharArray.sorted.mkString("").equals(nodeStatement.toCharArray.sorted.mkString("")))
  }

  test("test toDeleteSentence for edge") {
    val edges : ListBuffer[NebulaEdge] = new ListBuffer[NebulaEdge]
    val edgeType                       = "friend"
    val props1: Map[String, String]    = Map(
      "col_string" -> "\"Tom\"",
      "col_fixed_string" -> "\"Bob\"",
      "col_bool" -> "true",
      "col_int" -> "10",
      "col_int64" -> "100",
      "col_double" -> "1.0",
      "col_date" -> "date(\"2021-11-12\")"
      )
    val props2: Map[String, String]    =
      Map(
        "col_string" -> "\"Bob\"",
        "col_fixed_string" -> "\"Tom\"",
        "col_bool" -> "false",
        "col_int" -> "20",
        "col_int64" -> "200",
        "col_double" -> "2.0",
        "col_date" -> "date(\"2021-05-01\")"
        )
    edges.append(NebulaEdge(Map("id" -> "\"Tom\""), Map("id" -> "\"Bob\""), props1))
    edges.append(NebulaEdge(Map("id" -> "\"Bob\""), Map("id" -> "\"Tom\""), props2))

    val fieldTypeMap: Map[String, String] =
      Map("col_string" -> "STRING", "col_fixed_string" -> "STRING", "col_bool" -> "BOOL", "col_int" -> "INT32", "col_int64" -> "INT64", "col_double" -> "DOUBLE", "col_date" -> "DATE")

    val nebulaEdges: NebulaEdges = NebulaEdges(edgeType, "person", List("id"), Map("id" -> "STRING"), List("col_string"), "person", List("id"), Map("id" -> "STRING"), List("col_fixed_string"), List(), edges.toList, fieldTypeMap)

    val edgeStatement = NebulaExecutor.toDeleteSentence(graphName, edgeType, nebulaEdges, true)

    val expectStatement =
      s"""
         |TABLE t {`col_string`,`col_fixed_string`,`col_bool`,`col_int`,`col_int64`,`col_double`,`col_date`} =
         |(\"Tom\",\"Bob\",true,10,100,1.0,date(\"2021-11-12\")),(\"Bob\",\"Tom\",false,20,200,2.0,date(\"2021-05-01\"))
         |USE `$graphName`
         |FOR r IN t
         |RETURN r.`col_string` as _col_string,r.`col_fixed_string` as _col_fixed_string,r.`col_bool` as _col_bool,r.`col_int` as _col_int,r.`col_int64` as _col_int64,r.`col_double` as _col_double, r.`col_date` as _col_date
         |NEXT
         |USE `$graphName`
         |MATCH (nebula_src_node@`person`) WHERE nebula_src_node.`id`=CAST(_col_string AS STRING)
         |MATCH (nebula_dst_node@`person`) WHERE nebula_dst_node.`id`=CAST(_col_fixed_string AS STRING)
         |MATCH (nebula_src_node)-[e@`friend`]->(nebula_dst_node)
         |DELETE e
         |""".stripMargin
    assert(expectStatement.toCharArray.sorted.mkString("").trim.equals(edgeStatement.toCharArray.sorted.mkString("").trim))
  }


  test("test toDeleteSentence for edge with multiEdgeKeys") {
    val edges : ListBuffer[NebulaEdge] = new ListBuffer[NebulaEdge]
    val edgeType                       = "friend"
    val props1: Map[String, String]    = Map(
      "col_string" -> "\"Tom\"",
      "col_fixed_string" -> "\"Bob\"",
      "col_bool" -> "true",
      "col_int" -> "10",
      "col_int64" -> "100",
      "col_double" -> "1.0",
      "col_date" -> "date(\"2021-11-12\")"
      )
    val props2: Map[String, String]    =
      Map(
        "col_string" -> "\"Bob\"",
        "col_fixed_string" -> "\"Tom\"",
        "col_bool" -> "false",
        "col_int" -> "20",
        "col_int64" -> "200",
        "col_double" -> "2.0",
        "col_date" -> "date(\"2021-05-01\")"
        )
    edges.append(NebulaEdge(Map("id" -> "\"Tom\""), Map("id" -> "\"Bob\""), props1))
    edges.append(NebulaEdge(Map("id" -> "\"Bob\""), Map("id" -> "\"Tom\""), props2))

    val fieldTypeMap: Map[String, String] =
      Map("col_string" -> "STRING", "col_fixed_string" -> "STRING", "col_bool" -> "BOOL", "col_int" -> "INT32", "col_int64" -> "INT64", "col_double" -> "DOUBLE", "col_date" -> "DATE")

    val nebulaEdges: NebulaEdges = NebulaEdges(edgeType, "person", List("id"), Map("id" -> "STRING"), List("col_string"), "person", List("id"), Map("id" -> "STRING"), List("col_fixed_string"), List("col_int"), edges.toList, fieldTypeMap)

    val edgeStatement = NebulaExecutor.toDeleteSentence(graphName, edgeType, nebulaEdges, true)

    val expectStatement =
      s"""
         |TABLE t {`col_string`,`col_fixed_string`,`col_bool`,`col_int`,`col_int64`,`col_double`,`col_date`} =
         |(\"Tom\",\"Bob\",true,10,100,1.0,date(\"2021-11-12\")),(\"Bob\",\"Tom\",false,20,200,2.0,date(\"2021-05-01\"))
         |USE `$graphName`
         |FOR r IN t
         |RETURN r.`col_string` as _col_string,r.`col_fixed_string` as _col_fixed_string,r.`col_bool` as _col_bool,r.`col_int` as _col_int,r.`col_int64` as _col_int64,r.`col_double` as _col_double, r.`col_date` as _col_date
         |NEXT
         |USE `$graphName`
         |MATCH (nebula_src_node@`person`) WHERE nebula_src_node.`id`=CAST(_col_string AS STRING)
         |MATCH (nebula_dst_node@`person`) WHERE nebula_dst_node.`id`=CAST(_col_fixed_string AS STRING)
         |MATCH (nebula_src_node)-[e@`friend`]->(nebula_dst_node) FILTER e.`col_int`=CAST(_col_int AS INT32)
         |DELETE e
         |""".stripMargin
    assert(expectStatement.toCharArray.sorted.mkString("").trim.equals(edgeStatement.toCharArray.sorted.mkString("").trim))
  }


  test("test toUpdateSentence for edge") {
    val edges : ListBuffer[NebulaEdge] = new ListBuffer[NebulaEdge]
    val edgeType                       = "friend"
    val props1: Map[String, String]    = Map(
      "col_string" -> "\"Tom\"",
      "col_fixed_string" -> "\"Bob\"",
      "col_bool" -> "true",
      "col_int" -> "10",
      "col_int64" -> "100",
      "col_double" -> "1.0",
      "col_date" -> "date(\"2021-11-12\")"
      )
    val props2: Map[String, String]    =
      Map(
        "col_string" -> "\"Bob\"",
        "col_fixed_string" -> "\"Tom\"",
        "col_bool" -> "false",
        "col_int" -> "20",
        "col_int64" -> "200",
        "col_double" -> "2.0",
        "col_date" -> "date(\"2021-05-01\")"
        )
    edges.append(NebulaEdge(Map("id" -> "\"Tom\""), Map("id" -> "\"Bob\""), props1))
    edges.append(NebulaEdge(Map("id" -> "\"Bob\""), Map("id" -> "\"Tom\""), props2))

    val fieldTypeMap: Map[String, String] =
      Map("col_string" -> "STRING", "col_fixed_string" -> "STRING", "col_bool" -> "BOOL", "col_int" -> "INT32", "col_int64" -> "INT64", "col_double" -> "DOUBLE", "col_date" -> "DATE")

    val nebulaEdges: NebulaEdges = NebulaEdges(edgeType, "person", List("id"), Map("id" -> "STRING"), List("col_string"), "person", List("id"), Map("id" -> "STRING"), List("col_fixed_string"), List(), edges.toList, fieldTypeMap)

    val edgeStatement = NebulaExecutor.toUpdateSentence(graphName, edgeType, nebulaEdges, true)

    val expectStatement =
      s"""
         |TABLE t {`col_string`,`col_fixed_string`,`col_bool`,`col_int`,`col_int64`,`col_double`,`col_date`} =
         |(\"Tom\",\"Bob\",true,10,100,1.0,date(\"2021-11-12\")),(\"Bob\",\"Tom\",false,20,200,2.0,date(\"2021-05-01\"))
         |USE `$graphName`
         |FOR r IN t
         |RETURN r.`col_string` as _col_string,r.`col_fixed_string` as _col_fixed_string,r.`col_bool` as _col_bool,r.`col_int` as _col_int,r.`col_int64` as _col_int64,r.`col_double` as _col_double, r.`col_date` as _col_date
         |NEXT
         |USE `$graphName`
         |MATCH (nebula_src_node_pk@`person`) WHERE nebula_src_node_pk.`id`=CAST(_col_string AS STRING)
         |MATCH (nebula_dst_node_pk@`person`) WHERE nebula_dst_node_pk.`id`=CAST(_col_fixed_string AS STRING)
         |MATCH (nebula_src_node_pk)-[e@`friend`]->(nebula_dst_node_pk)
         |SET e.`col_string`=CAST(_col_string AS STRING),e.`col_fixed_string`=CAST(_col_fixed_string AS STRING),e.`col_bool`=CAST(_col_bool AS BOOL),e.`col_date`=CAST(_col_date AS DATE),e.`col_int64`=CAST(_col_int64 AS INT64),e.`col_int`=CAST(_col_int AS INT32),e.`col_double`=CAST(_col_double AS DOUBLE)
         |""".stripMargin
    assert(expectStatement.toCharArray.sorted.mkString("").trim.equals(edgeStatement.toCharArray.sorted.mkString("").trim))
  }

  test("test toUpdateSentence for edge with multiEdgeKeys") {
    val edges : ListBuffer[NebulaEdge] = new ListBuffer[NebulaEdge]
    val edgeType                       = "friend"
    val props1: Map[String, String]    = Map(
      "col_string" -> "\"Tom\"",
      "col_fixed_string" -> "\"Bob\"",
      "col_bool" -> "true",
      "col_int" -> "10",
      "col_int64" -> "100",
      "col_double" -> "1.0",
      "col_date" -> "date(\"2021-11-12\")"
      )
    val props2: Map[String, String]    =
      Map(
        "col_string" -> "\"Bob\"",
        "col_fixed_string" -> "\"Tom\"",
        "col_bool" -> "false",
        "col_int" -> "20",
        "col_int64" -> "200",
        "col_double" -> "2.0",
        "col_date" -> "date(\"2021-05-01\")"
        )
    edges.append(NebulaEdge(Map("id" -> "\"Tom\""), Map("id" -> "\"Bob\""), props1))
    edges.append(NebulaEdge(Map("id" -> "\"Bob\""), Map("id" -> "\"Tom\""), props2))

    val fieldTypeMap: Map[String, String] =
      Map("col_string" -> "STRING", "col_fixed_string" -> "STRING", "col_bool" -> "BOOL", "col_int" -> "INT32", "col_int64" -> "INT64", "col_double" -> "DOUBLE", "col_date" -> "DATE")

    val nebulaEdges: NebulaEdges = NebulaEdges(edgeType, "person", List("id"), Map("id" -> "STRING"), List("col_string"), "person", List("id"), Map("id" -> "STRING"), List("col_fixed_string"), List("col_int","col_int64"), edges.toList, fieldTypeMap)

    val edgeStatement = NebulaExecutor.toUpdateSentence(graphName, edgeType, nebulaEdges, true)

    val expectStatement =
      s"""
         |TABLE t {`col_string`,`col_fixed_string`,`col_bool`,`col_int`,`col_int64`,`col_double`,`col_date`} =
         |(\"Tom\",\"Bob\",true,10,100,1.0,date(\"2021-11-12\")),(\"Bob\",\"Tom\",false,20,200,2.0,date(\"2021-05-01\"))
         |USE `$graphName`
         |FOR r IN t
         |RETURN r.`col_string` as _col_string,r.`col_fixed_string` as _col_fixed_string,r.`col_bool` as _col_bool,r.`col_int` as _col_int,r.`col_int64` as _col_int64,r.`col_double` as _col_double, r.`col_date` as _col_date
         |NEXT
         |USE `$graphName`
         |MATCH (nebula_src_node_pk@`person`) WHERE nebula_src_node_pk.`id`=CAST(_col_string AS STRING)
         |MATCH (nebula_dst_node_pk@`person`) WHERE nebula_dst_node_pk.`id`=CAST(_col_fixed_string AS STRING)
         |MATCH (nebula_src_node_pk)-[e@`friend`]->(nebula_dst_node_pk) FILTER e.`col_int64`=CAST(_col_int64 AS INT64) AND e.`col_int`=CAST(_col_int AS INT32)
         |SET e.`col_string`=CAST(_col_string AS STRING),e.`col_fixed_string`=CAST(_col_fixed_string AS STRING),e.`col_bool`=CAST(_col_bool AS BOOL),e.`col_date`=CAST(_col_date AS DATE),e.`col_double`=CAST(_col_double AS DOUBLE)
         |""".stripMargin
    assert(expectStatement.toCharArray.sorted.mkString("").trim.equals(edgeStatement.toCharArray.sorted.mkString("").trim))
  }
}
