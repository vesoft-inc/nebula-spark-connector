/*
 * Copyright (c) 2025 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

package com.vesoft.nebula.spark.common


import scala.collection.mutable.ListBuffer

case class NebulaNode(values: Map[String, String])

case class NebulaNodes(nodeType: String, values: List[NebulaNode], pkNames: List[String], fieldTypeMap: Map[String, String]) {
  private val propNames = values.iterator.next().values.keySet.toSeq

  /** *
   * construct the table header
   */
  def tableHeaders: String = propNames.map(p=>s"`$p`").mkString(",")

  /**
   * construct the table value
   */
  def getNodesStr = values.map(node => {
    s"(${propNames.map(prop => s"${node.values(prop)}").mkString(",")})"
  }).mkString(",")


  /**
   * construct the property mapping statement for insert
   */
  def propNamesWithTableStr = propNames.map(prop => s"`$prop`:CAST(r.`$prop` AS ${fieldTypeMap(prop)})").mkString(",")

  /**
   * construct the match filter statement for node
   */
  def getPkFieldMathStatement(prefix: String): String = {
    val srcPkMappings = new ListBuffer[String]
    for (pkName <- pkNames) {
      srcPkMappings.append(s"$prefix.`${pkName}`=CAST(r.`${pkName}` AS ${fieldTypeMap(pkName)})")
    }
    srcPkMappings.mkString(" AND ")
  }

  /**
   * construct the setting property mapping for update
   */
  def getUpdatePropNamesWithTableStr(prefix: String) = {
    val propNameMappings = new ListBuffer[String]
    for (name <- propNames) {
      if (!pkNames.contains(name)) {
        propNameMappings.append(s"$prefix.`${name}`=CAST(r.`${name}` AS ${fieldTypeMap(name)})")
      }
    }
    propNameMappings.mkString(",")
  }

}

/**
 * NebulaEdge struct
 * srcIds: src property name -> src property value
 * dstIds: dst property name -> dst property value
 * values: edge property name -> edge property value
 */
case class NebulaEdge(srcIds: Map[String, String], dstIds: Map[String, String], values: Map[String, String])

case class NebulaEdges(edgeType: String,
                       srcType: String,
                       srcPkNames: List[String],
                       srcPkDataTypeMap: Map[String, String],
                       dfSrcFields: List[String],
                       dstType: String,
                       dstPkNames: List[String],
                       dstPkDataTypeMap: Map[String, String],
                       dfDstFields: List[String],
                       values: List[NebulaEdge],
                       fieldTypeMap: Map[String, String]) {
  private val propNames = values.iterator.next().values.keySet.toSeq

  def tableHeaders: String = {
    val propNamesWithoutPks = propNames.filterNot(p => dfSrcFields.contains(p) || dfDstFields.contains(p))
    val pksAndPropNames     = dfSrcFields ++ dfDstFields ++ propNamesWithoutPks
    pksAndPropNames.distinct.map(p =>s"`$p`").mkString(",")
  }

  def getEdgesStr = {
    val delimiter = if (propNames == null || propNames.isEmpty) "" else ","

    values.map(edge => {
      // construct the src node pks value
      val srcValues = new ListBuffer[String]
      srcPkNames.foreach(pk => {
        srcValues.append(edge.srcIds(pk))
      })
      // construct the dst node pks value, if dst pk has the same value with src pk, skip it.
      val dstValues = new ListBuffer[String]
      dstPkNames.foreach(pk => {
       if(!dfSrcFields.contains(dfDstFields(dstPkNames.indexOf(pk)))){
         dstValues.append(edge.dstIds(pk))
       }
      })

      s"(${srcValues.mkString(",")},${dstValues.mkString(",")}$delimiter" +
        s"${
          propNames
            .filterNot(p => dfSrcFields.contains(p) || dfDstFields.contains(p))
            .map(prop => s"${edge.values(prop)}")
            .mkString(",")
        })"
    }).mkString(",")
  }

  /**
   * construct the new table headers with alias the header name
   * such as: return r.a as a,r.b as b
   */
  def getNewTableHeaders: String = {
    val propNamesWithoutPks = propNames.filterNot(p => dfSrcFields.contains(p) || dfDstFields.contains(p))
    val pksAndPropNames     = dfSrcFields ++ dfDstFields ++ propNamesWithoutPks
    pksAndPropNames.distinct.map(n => s"r.`$n` as _$n").mkString(",")
  }

  def getSrcPkStr(prefix: String): String = {
    val srcPkMappings = new ListBuffer[String]
    for (i <- srcPkNames.indices) {
      srcPkMappings.append(s"$prefix.`${srcPkNames(i)}`=CAST(_${dfSrcFields(i)} AS ${srcPkDataTypeMap(srcPkNames(i))})")
    }
    srcPkMappings.mkString(" AND ")
  }

  def getDstPkStr(prefix: String): String = {
    val dstPkMappings = new ListBuffer[String]
    for (i <- dstPkNames.indices) {
      dstPkMappings.append(s"$prefix.`${dstPkNames(i)}`=CAST(_${dfDstFields(i)} AS ${dstPkDataTypeMap(dstPkNames(i))})")
    }
    dstPkMappings.mkString(" AND ")
  }


  def propNamesWithTableStr: String = propNames.map(prop => s"`$prop`:CAST(_$prop AS ${fieldTypeMap(prop)})").mkString(",")

  /**
   * construct the setting property mapping for update
   */
  def getUpdatePropNamesWithTableStr: String = propNames.map(prop => s"e.`$prop`=CAST(_$prop AS ${fieldTypeMap(prop)})").mkString(",")

}
