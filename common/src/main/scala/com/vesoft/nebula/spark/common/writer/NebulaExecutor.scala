/*
 * Copyright (c) 2025 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

package com.vesoft.nebula.spark.common.writer

import com.vesoft.nebula.spark.common.{NebulaEdges, NebulaNodes, NebulaUtils}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types.StructType
import org.slf4j.LoggerFactory

import scala.collection.mutable

object NebulaExecutor {

  private val LOG = LoggerFactory.getLogger(this.getClass)

  private val VECTOR     = "VECTOR<"
  private val LIST       = "LIST<"
  private val STRINGLIST = "LIST<STRING"
  private val SET        = "SET<"
  private val STRINGSET  = "SET<STRING"
  private val MAP        = "MAP<"
  private val GEOGRAPHY  = "GEOGRAPHY"


  /**
   * deal with node property values
   *
   * @param schema
   * @param record
   * @param fieldTypeMap
   * @return Map of property name to property value
   * */
  def assignNodePropValues(schema: StructType,
                           record: InternalRow,
                           fieldTypeMap: Map[String, String]): Map[String, String] = {
    val properties: mutable.HashMap[String, String] = new mutable.HashMap[String, String]()
    for {
      index <- schema.fields.indices
    } yield {
      properties += (schema.fields(index).name -> extraValue(record, schema, index, fieldTypeMap))
    }
    properties.toMap
  }

  /**
   * deal with node pk value
   * this function just need to deal with the primary key, not all the properties
   *
   * @param schema       spark dataframe schema
   * @param record       spark dataframe row
   * @param fieldTypeMap field name and data type map in NebulaGraph
   * @param pkNames      primary key property names
   * @return Map of pk name to property value
   */
  def assignNodePkValues(schema: StructType,
                         record: InternalRow,
                         fieldTypeMap: Map[String, String],
                         pkNames: List[String]): Map[String, String] = {
    val properties: mutable.HashMap[String, String] = new mutable.HashMap[String, String]()
    pkNames.foreach(pkName => {
      val index = schema.fieldIndex(pkName)
      properties += (pkName -> extraValue(record, schema, index, fieldTypeMap))
    })
    properties.toMap
  }

  /**
   * deal with edge property values
   *
   * @param schema
   * @param record
   * @param srcIndex
   * @param dstIndex
   * @param fieldTypeMap
   */
  def assignEdgeValues(schema: StructType,
                       record: InternalRow,
                       srcIndices: List[Int],
                       dstIndices: List[Int],
                       srcAsProp: Boolean,
                       dstAsProp: Boolean,
                       fieldTypeMap: Map[String, String]): Map[String, String] = {
    val properties: mutable.HashMap[String, String] = new mutable.HashMap[String, String]()
    for {
      index <- schema.fields.indices
      if (srcAsProp || !srcIndices.contains(index)) && (dstAsProp || !dstIndices.contains(index))
    } yield {
      properties += (schema.fields(index).name -> extraValue(record, schema, index, fieldTypeMap))
    }
    properties.toMap
  }

  /**
   * get and convert primary key value
   *
   * @param record   DataFrame internal row
   * @param schema   DataFrame schema
   * @param index    the position of row columns
   * @param dataType datatype of property in nebula
   */
  def extraPropValue(record: InternalRow,
                     schema: StructType,
                     index: Int,
                     dataType: String): String = {
    if (record.isNullAt(index)) return null

    val types     = schema.fields.map(field => field.dataType)
    val propValue = record.get(index, types(index))
    if (propValue.toString.isEmpty && !"STRING".equals(dataType)) {
      return null
    }

    dataType match {
      case "STRING" =>
        NebulaUtils.escapeUtil(propValue.toString).mkString("\"", "", "\"")
      case "DATE" => "date(\"" + propValue + "\")"
      case "LOCAL DATETIME" => "local_datetime(\"" + propValue + "\")"
      case "LOCAL TIME" => "local_time(\"" + propValue + "\")"
      case "ZONED TIME" => "zoned_time(\"" + propValue + "\")"
      case "ZONED DATETIME" => "zoned_datetime(\"" + propValue + "\")"
      case "INT8" | "INT16" | "INT32" | "INT64" | "FLOAT" | "DOUBLE" | "BOOL" => propValue.toString
      case _ =>
        // convert value to list, only support the source value format: [xxx, xxx, xxx]
        if (dataType.startsWith(LIST)) {
          if (dataType.startsWith(STRINGLIST)) {
            val sb = new StringBuilder()
            sb.append("LIST[")
            val trimmedInput                       = propValue.toString.stripPrefix("[").stripSuffix("]")
            val pattern: scala.util.matching.Regex = """(['"])((?:\\\1|.)*?)\1|([^,]+)""".r
            val matches                            = pattern.findAllMatchIn(trimmedInput)
            while (matches.hasNext) {
              val regexMatch = matches.next()
              if (regexMatch.group(1) != null) {
                sb.append("\"").append(NebulaUtils.escapeUtil(regexMatch.group(2).replace(s"\\${regexMatch.group(1)}", regexMatch.group(1)))).append("\"").append(",")
              } else {
                sb.append("\"").append(NebulaUtils.escapeUtil(regexMatch.group(3))).append("\"").append(",")
              }
            }
            if (sb.length > 5) {
              sb.deleteCharAt(sb.length - 1)
            }
            sb.append("]")
            return sb.toString()
          } else {
            return s"""LIST$propValue"""
          }
        }
        // convert value to set, only support the source value format: {xxx, xxx, xxx}
        if (dataType.startsWith(SET)) {
          if (dataType.startsWith(STRINGSET)) {
            val sb = new StringBuilder()
            sb.append("SET{")
            val trimmedInput                       = propValue.toString.stripPrefix("\"")
              .stripSuffix("\"")
              .stripPrefix("{")
              .stripSuffix("}")
            val pattern: scala.util.matching.Regex = """\s*(?:['"]([^'"]*)['"]|([^,]+))\s*""".r
            val matches                            = pattern.findAllMatchIn(trimmedInput)
            while (matches.hasNext) {
              val regexMatch = matches.next()
              val content    = if (regexMatch.group(1) != null) {
                NebulaUtils.escapeUtil(regexMatch.group(1))
              } else {
                NebulaUtils.escapeUtil(regexMatch.group(0))
              }
              sb.append("'").append(content).append("',")
            }
            if (sb.length > 5) {
              sb.deleteCharAt(sb.length - 1)
            }
            sb.append("}")
            return sb.toString()
          } else {
            return s"""SET$propValue"""
          }
        }
        // convert value to map
        if (dataType.startsWith(MAP)) {
          return s"""MAP$propValue"""
        }
        // convert value to vector
        if (dataType.startsWith(VECTOR)) {
          return s"""$dataType($propValue)"""
        }
        if (dataType.startsWith(GEOGRAPHY)) {
          return s"""ST_GeogFromText("$propValue")"""
        }
        val propValueTypeClassName = propValue.getClass.getName
        val simpleName             = propValueTypeClassName.substring(propValueTypeClassName.lastIndexOf(".") + 1,
                                                                      propValueTypeClassName.length)
        if (simpleName.equalsIgnoreCase("UTF8String")) propValue.toString
        else propValue.toString
    }
  }

  /**
   * get and convert property value
   *
   * @param record       DataFrame internal row
   * @param schema       DataFrame schema
   * @param index        the position of row columns
   * @param fieldTypeMap property name -> property datatype in nebula
   */
  def extraValue(record: InternalRow,
                 schema: StructType,
                 index: Int,
                 fieldTypeMap: Map[String, String]): String = {
    val fieldName = schema.fields(index).name
    if (!fieldTypeMap.contains(fieldName)) {
      throw new RuntimeException(s"NebulaGraph does not contain the property ${fieldName}")
    }
    extraPropValue(record, schema, index, fieldTypeMap(fieldName))
  }

  /**
   * construct insert statement for node
   * TABLE t {id,firstName,lastName, tag_name} =
   * (1, "f1", "l1", "tag1"),
   * (2, "f2", "l2", "tag2"),
   * (3, "f3", "l3", "tag3")
   * USE nba
   * FOR r IN t
   * INSERT (a@Person{id:r.id,firstName:r.firstName,lastName:r.lastName})
   */
  def toInsertSentence(graphName: String, nodes: NebulaNodes, mode: String): String = {
    s"""
       |TABLE t {${nodes.tableHeaders}} =
       |${nodes.getNodesStr}
       |USE `$graphName`
       |FOR r IN t
       |INSERT $mode (@`${nodes.nodeType}`{${nodes.propNamesWithTableStr}})
       |""".stripMargin
  }

  /**
   * construct insert statement for edge
   * TABLE t {id1, id2, degree} =
   * (1,2,10),
   * (2,3,20)
   * USE nba
   * FOR r IN t
   * RETURN r.id1 as id1, r.id2 as id2, r.degree as degree
   * MATCH (src@Person) WHERE src.`id`=CAST(id1 AS INT64)
   * MATCH (dst:@Person) WHERE dst.`id`=CAST(id2 AS INT64)
   * INSERT (src)-[e@friend{degree:degree}]->(dst)
   */
  def toInsertSentence(graphName: String, edges: NebulaEdges, mode: String, isDirected: Boolean): String = {
    if (isDirected) {
      s"""
         |TABLE t {${edges.tableHeaders}} =
         |${edges.getEdgesStr}
         |USE `$graphName`
         |FOR r IN t
         |RETURN ${edges.getNewTableHeaders}
         |NEXT
         |USE `$graphName`
         |OPTIONAL MATCH (src_node@`${edges.srcType}`) WHERE ${edges.getSrcPkStr("src_node")}
         |OPTIONAL MATCH (dst_node@`${edges.dstType}`) WHERE ${edges.getDstPkStr("dst_node")}
         |INSERT $mode (src_node)-[e@`${edges.edgeType}`{${edges.propNamesWithTableStr}}]->(dst_node)
         |""".stripMargin
    } else {
      s"""
         |TABLE t {${edges.tableHeaders}} =
         |${edges.getEdgesStr}
         |USE `$graphName`
         |FOR r IN t
         |RETURN ${edges.getNewTableHeaders}
         |NEXT
         |USE `$graphName`
         |OPTIONAL MATCH (src_node@`${edges.srcType}`) WHERE ${edges.getSrcPkStr("src_node")}
         |OPTIONAL MATCH (dst_node@`${edges.dstType}`) WHERE ${edges.getDstPkStr("dst_node")}
         |INSERT $mode (src_node)~[e@`${edges.edgeType}`{${edges.propNamesWithTableStr}}]~(dst_node)
         |""".stripMargin
    }
  }

  /**
   * construct update statement for node
   */
  def toUpdateSentence(graphName: String, nodeType: String, nodes: NebulaNodes): String = {
    s"""
       |TABLE t {${nodes.tableHeaders}} =
       |${nodes.getNodesStr}
       |USE `$graphName`
       |FOR r IN t
       |OPTIONAL MATCH(v_node@`${nodes.nodeType}`) WHERE ${nodes.getPkFieldMathStatement("v_node")}
       |SET ${nodes.getUpdatePropNamesWithTableStr("v_node")}
       |""".stripMargin
  }


  /**
   * construct update statement for edge
   */
  def toUpdateSentence(graphName: String, edgeType: String, edges: NebulaEdges, isDirected: Boolean): String = {
    if (isDirected) {
      s"""
         |TABLE t {${edges.tableHeaders}} =
         |${edges.getEdgesStr}
         |USE `$graphName`
         |FOR r IN t
         |RETURN ${edges.getNewTableHeaders}
         |NEXT
         |USE `$graphName`
         |MATCH (nebula_src_node_pk@`${edges.srcType}`) WHERE ${edges.getSrcPkStr("nebula_src_node_pk")}
         |MATCH (nebula_dst_node_pk@`${edges.dstType}`) WHERE ${edges.getDstPkStr("nebula_dst_node_pk")}
         |MATCH (nebula_src_node_pk)-[e@`${edges.edgeType}`${edges.getMultiEdgeKeysWithTableStr}]->(nebula_dst_node_pk)
         |SET ${edges.getUpdatePropNamesWithTableStr}
         |""".stripMargin
    } else {
      s"""
         |TABLE t {${edges.tableHeaders}} =
         |${edges.getEdgesStr}
         |USE `$graphName`
         |FOR r IN t
         |RETURN ${edges.getNewTableHeaders}
         |NEXT
         |USE `$graphName`
         |MATCH (nebula_src_node_pk@`${edges.srcType}`) WHERE ${edges.getSrcPkStr("nebula_src_node_pk")}
         |MATCH (nebula_dst_node_pk@`${edges.dstType}`) WHERE ${edges.getDstPkStr("nebula_dst_node_pk")}
         |MATCH (nebula_src_node_pk)~[e@`${edges.edgeType}`${edges.getMultiEdgeKeysWithTableStr}]~(nebula_dst_node_pk)
         |SET ${edges.getUpdatePropNamesWithTableStr}
         |""".stripMargin
    }
  }


  /**
   * construct delete statement for node
   * USE graphName MATCH((a@nodeType where a.id in [ 1,2,3,4,5]-[r]-(b)) DETACH DELETE a
   */
  def toDeleteSentence(graphName: String, nodeType: String, nodes: NebulaNodes, deleteMode: String): String = {
    if (nodes.pkNames.size == 1) {
      val nodePks = nodes.values.map(node => node.values(nodes.pkNames.head)).mkString(",")
      s"USE `$graphName` MATCH(a@`$nodeType` where a.`${nodes.pkNames.head}` in [$nodePks]) DETACH DELETE a "
    } else {
      s"""
         |TABLE t {${nodes.tableHeaders}} =
         |${nodes.getNodesStr}
         |USE `$graphName`
         |FOR r IN t
         |MATCH(v_node@`${nodes.nodeType}`) WHERE ${nodes.getPkFieldMathStatement("v_node")}
         |${deleteMode} v_node
         |""".stripMargin
    }

  }


  /**
   * construct delete statement for edge
   */
  def toDeleteSentence(graphName: String, edgeType: String, edges: NebulaEdges, isDirected: Boolean): String = {
    if (isDirected) {
      s"""
         |TABLE t {${edges.tableHeaders}} =
         |${edges.getEdgesStr}
         |USE `$graphName`
         |FOR r IN t
         |RETURN ${edges.getNewTableHeaders}
         |NEXT
         |USE `$graphName`
         |MATCH (nebula_src_node@`${edges.srcType}`) WHERE ${edges.getSrcPkStr("nebula_src_node")}
         |MATCH (nebula_dst_node@`${edges.dstType}`) WHERE ${edges.getDstPkStr("nebula_dst_node")}
         |MATCH (nebula_src_node)-[e@`${edges.edgeType}`${edges.getMultiEdgeKeysWithTableStr}]->(nebula_dst_node)
         |DELETE e
         |""".stripMargin
    } else {
      s"""
         |TABLE t {${edges.tableHeaders}} =
         |${edges.getEdgesStr}
         |USE `$graphName`
         |FOR r IN t
         |RETURN ${edges.getNewTableHeaders}
         |NEXT
         |USE `$graphName`
         |MATCH (nebula_src_node@`${edges.srcType}`) WHERE ${edges.getSrcPkStr("nebula_src_node")}
         |MATCH (nebula_dst_node@`${edges.dstType}`) WHERE ${edges.getDstPkStr("nebula_dst_node")}
         |MATCH (nebula_src_node)~[e@`${edges.edgeType}`${edges.getMultiEdgeKeysWithTableStr}]~(nebula_dst_node)
         |DELETE e
         |""".stripMargin
    }
  }

  /**
   * escape nebula property name, add `` for each property.
   *
   * @param nebulaFields nebula property name list
   * @return escaped nebula property name list
   */
  def escapePropName(nebulaFields: List[String]): List[String] = nebulaFields.map(key => s"`$key`")

}
