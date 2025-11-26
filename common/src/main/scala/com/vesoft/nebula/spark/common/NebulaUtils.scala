/*
 * Copyright (c) 2025 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

package com.vesoft.nebula.spark.common

import com.vesoft.nebula.driver.graph.data.ResultSet
import com.vesoft.nebula.spark.common.nebula.{EdgeDesc, GraphProvider, NodeDesc}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String
import org.slf4j.LoggerFactory

import scala.collection.JavaConverters.asScalaBufferConverter
import scala.collection.mutable.ListBuffer

object NebulaUtils {
  private val LOG = LoggerFactory.getLogger(this.getClass)


  type NebulaValueGetter = (Any, InternalRow, Int) => Unit

  /**
   * make getter
   *
   * @param schema Spark DataFrame schema
   * @return list of NebulaValueGetter
   */
  def makeGetters(schema: StructType): Array[NebulaValueGetter] = {
    schema.fields.map(field => makeGetter(field.dataType))
  }

  private def makeGetter(dataType: DataType): NebulaValueGetter = {
    dataType match {
      case BooleanType =>
        (prop: Any, row: InternalRow, pos: Int) =>
          row.setBoolean(pos, prop.asInstanceOf[Boolean])
      case TimestampType | LongType =>
        (prop: Any, row: InternalRow, pos: Int) =>
          row.setLong(pos, prop.asInstanceOf[Long])
      case FloatType | DoubleType =>
        (prop: Any, row: InternalRow, pos: Int) =>
          row.setDouble(pos, prop.asInstanceOf[Double])
      case IntegerType =>
        (prop: Any, row: InternalRow, pos: Int) =>
          row.setInt(pos, prop.asInstanceOf[Int])
      case _ =>
        (prop: Any, row: InternalRow, pos: Int) =>
          row.update(pos, UTF8String.fromString(String.valueOf(prop)))
    }
  }

  /**
   * check if a str is numic
   *
   * @param str string
   * @return true if str is numic
   */
  def isNumic(str: String): Boolean =
    str.matches("-?\\d+")

  /**
   * escape the string which contains escape str
   *
   * @param str string
   * @return escaped string
   */
  def escapeUtil(str: String): String =
    str
      .replaceAll("\\\\", "\\\\\\\\")
      .replaceAll("\t", "\\\\t")
      .replaceAll("\n", "\\\\n")
      .replaceAll("\"", "\\\\\"")
      .replaceAll("\'", "\\\\'")
      .replaceAll("\r", "\\\\r")
      .replaceAll("\b", "\\\\b")


  /**
   * return the dataset's schema.
   * schema includes configured cols in returnCols, if returnCols is null, return all the properties.
   * if returnCols is empty, return no properties but just vid for node and srcId, dstId for edge.
   *
   * for node, the pk name always be the first position of schema.
   * for edge, the schema fields are: src pk name, dst pk name, edge properties name
   *
   * @param nebulaOptions operations for schema
   * @return StructType
   */
  def getSchema(nebulaOptions: NebulaOptions): StructType = {
    var returnCols    = nebulaOptions.getReturnCols
    val graphProvider = new GraphProvider(nebulaOptions)
    val isNodeType    = DataTypeEnum.NODE.toString.equalsIgnoreCase(nebulaOptions.dataType)

    val fields: ListBuffer[StructField] = new ListBuffer[StructField]
    try {
      if (isNodeType) {
        val nodeDesc = graphProvider.getNodeDesc(nebulaOptions.graphName, nebulaOptions.label)
        val pks      = nodeDesc.nodePkNames
        pks.foreach(pk => {
          fields.append(DataTypes.createStructField(pk, DataTypes.StringType, false))
        })
        // if returnCols is null, read all the property of node type/edge type
        if (returnCols == null) {
          returnCols = nodeDesc.propNames.toList
        }
        // add node returnCols name to Spark schema's fields
        for (propName <- returnCols) {
          if (!pks.contains(propName)) {
            fields.append(DataTypes.createStructField(propName, DataTypes.StringType, true))
          }
        }
        new StructType(fields.toArray)
      } else {
        val edgeDesc = graphProvider.getEdgeDesc(nebulaOptions.graphName, nebulaOptions.label)
        edgeDesc.srcNodePkNames.foreach(srcPk => {
          fields.append(DataTypes.createStructField(s"src_$srcPk", DataTypes.StringType, false))
        })
        edgeDesc.dstNodePkNames.foreach(dstPk => {
          fields.append(DataTypes.createStructField(s"dst_$dstPk", DataTypes.StringType, false))
        })
        // if returnCols is null, read all the property of node type/edge type
        if (returnCols == null) {
          returnCols = edgeDesc.propNames.toList
        }
        // add edge returnCols name to Spark schema's fields
        for (propName <- returnCols) {
          // if edge property has the same name with src/dst node's pk name, rename it with suffix $
          val finalPropName =
            if (edgeDesc.srcNodePkNames.contains(s"src_$propName") || edgeDesc.dstNodePkNames.contains(s"dst_$propName")) {
              propName + "$"
            } else {
              propName
            }
          fields.append(DataTypes.createStructField(finalPropName, DataTypes.StringType, true))
        }
        new StructType(fields.toArray)
      }
    } finally {
      graphProvider.close()
    }
  }


  /**
   * return the qgl result schema
   */
  def getSchemaForGql(nebulaOptions: NebulaOptions): StructType = {
    val fields: ListBuffer[StructField] = new ListBuffer[StructField]

    val gql = nebulaOptions.gql.trim

    val newGql =
      if (gql.toUpperCase.contains(" LIMIT ")) {
        val lowerGql   = gql.toLowerCase
        val limitIndex = lowerGql.indexOf("limit");
        var endIndex   = lowerGql.indexOf(" ", limitIndex + 6);
        endIndex = if (endIndex > 0) endIndex else gql.length()
        gql.substring(0, limitIndex) + "limit 1 " + gql.substring(endIndex);
      } else {
        gql + " limit 1"
      }


    LOG.info(s"new gql: $newGql")
    val graphProvider     = new GraphProvider(nebulaOptions)
    var result: ResultSet = null
    try {
      result = graphProvider.submit(newGql)
    } finally {
      graphProvider.close()
    }
    for (column <- result.getColumnNames.asScala) {
      fields.append(StructField(column, StringType, true))
    }
    StructType(fields)
  }


  /**
   * check if the fields in DataFrame match with property of NebulaGraph Edge
   *
   * @param nebulaOptions configs for NebulaGraph
   * @param schema        schema of DataFrame
   * @throws RuntimeException when the DataFrame's field name does not exist in NebulaGraph
   */
  def checkEdgeSchemaMatchWithDataFrame(nebulaOptions: NebulaOptions, schema: StructType): Unit = {
    val graphProvider      = new GraphProvider(nebulaOptions)
    var edgeDesc: EdgeDesc = null
    try {
      edgeDesc = graphProvider.getEdgeDesc(nebulaOptions.graphName, nebulaOptions.label)
    } finally {
      graphProvider.close()
    }
    val dataFrameFields = new ListBuffer[String]
    schema.fields.toList.foreach(field => {
      if ((!nebulaOptions.srcPkFields.contains(field.name) || nebulaOptions.srcPksAsProp)
        && (!nebulaOptions.dstPkFields.contains(field.name) || nebulaOptions.dstPksAsProp)) {
        dataFrameFields.append(field.name)
      }
    })
    for (field <- dataFrameFields) {
      assert(edgeDesc.properties.keySet.contains(field),
             s"the dataframe field $field does not match the properties of ${nebulaOptions.label}.")
    }
  }


  /**
   * check if the fields in DataFrame match with property of NebulaGraph Node
   *
   * @param nebulaOptions configs for NebulaGraph
   * @param schema        schema of DataFrame
   * @throws RuntimeException when the DataFrame's field name does not exist in NebulaGraph
   */
  def checkNodeSchemaMatchWithDataFrame(nebulaOptions: NebulaOptions, schema: StructType): Unit = {
    val graphProvider      = new GraphProvider(nebulaOptions)
    var nodeDesc: NodeDesc = null
    try {
      nodeDesc = graphProvider.getNodeDesc(nebulaOptions.graphName, nebulaOptions.label)
    } finally {
      graphProvider.close()
    }
    val dataFrameFields = new ListBuffer[String]
    schema.fields.toList.foreach(field => {
      dataFrameFields.append(field.name)
    })
    for (field <- dataFrameFields) {
      assert(nodeDesc.properties.keySet.contains(field),
             s"the dataframe field $field does not match the properties of ${nebulaOptions.label}.")
    }
  }
}
