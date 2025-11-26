/*
 * Copyright (c) 2025 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */
package com.vesoft.nebula.spark.common.reader

import com.vesoft.nebula.driver.graph.data.{ResultSet, ValueWrapper}
import com.vesoft.nebula.spark.common.NebulaUtils.NebulaValueGetter
import com.vesoft.nebula.spark.common.nebula.GraphProvider
import com.vesoft.nebula.spark.common.{NebulaOptions, NebulaUtils}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.SpecificInternalRow
import org.apache.spark.sql.types.{StringType, StructType}
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.JavaConverters.asScalaBufferConverter

trait NebulaGqlReader {
  private val LOG: Logger = LoggerFactory.getLogger(this.getClass)

  private var nebulaOptions: NebulaOptions = _
  private var graphProvider: GraphProvider = _
  private var resultSet    : ResultSet     = _
  private var schema       : StructType    = _

  def init(nebulaOptions: NebulaOptions) {
    LOG.info(s"query NebulaGraph through gql: ${nebulaOptions.gql}")
    this.nebulaOptions = nebulaOptions
    this.graphProvider = new GraphProvider(nebulaOptions)
    this.resultSet = graphProvider.submit(nebulaOptions.gql)
    schema = new StructType()
    for (name <- resultSet.getColumnNames.asScala) {
      schema = schema.add(name, StringType)
    }
    LOG.info(s"the gql result schema is: ${schema.fields.map(_.name).mkString(",")}")
  }

  def hasNext(): Boolean = this.resultSet.hasNext

  def getRow(): InternalRow = {
    val record : Array[ValueWrapper]      = resultSet.next().values().toArray.map(v => v.asInstanceOf[ValueWrapper])
    val getters: Array[NebulaValueGetter] = NebulaUtils.makeGetters(schema)
    val mutableRow                        = new SpecificInternalRow(schema.fields.map(x => x.dataType))
    // resolve the query result data
    for (i <- getters.indices) {
      val value: ValueWrapper = record(i)
      if (value.isNull) {
        mutableRow.setNullAt(i)
      }
      if (value.isString) {
        getters(i).apply(value.asString(), mutableRow, i)
      }
      if (value.isDate) {
        getters(i).apply(value.asDate().toString, mutableRow, i)
      }
      if (value.isLocalTime) {
        getters(i).apply(value.asLocalTime().toString, mutableRow, i)
      }
      if (value.isZonedTime) {
        getters(i).apply(value.asZonedTime().toString, mutableRow, i)
      }
      if (value.isLocalDateTime) {
        getters(i).apply(value.asLocalDateTime().toString, mutableRow, i)
      }
      if (value.isZonedDateTime) {
        getters(i).apply(value.asZonedDateTime().toString, mutableRow, i)
      }
      if (value.isInt) {
        getters(i).apply(value.asInt().toString, mutableRow, i)
      }
      if (value.isLong) {
        getters(i).apply(value.asLong().toString, mutableRow, i)
      }
      if (value.isBoolean) {
        getters(i).apply(value.asBoolean().toString, mutableRow, i)
      }
      if (value.isFloat) {
        getters(i).apply(value.asFloat().toString, mutableRow, i)
      }
      if (value.isDouble) {
        getters(i).apply(value.asDouble().toString, mutableRow, i)
      }
      if (value.isDuration) {
        getters(i).apply(value.asDuration().toString, mutableRow, i)
      }
      if (value.isList) {
        getters(i).apply(value.asList().toString, mutableRow, i)
      }
      if (value.isNode) {
        getters(i).apply(value.asNode().toString, mutableRow, i)
      }
      if (value.isEdge) {
        getters(i).apply(value.asEdge().toString, mutableRow, i)
      }
      if (value.isRecord) {
        getters(i).apply(value.asRecord().toString, mutableRow, i)
      }
      if (value.isPath) {
        getters(i).apply(value.asPath().toString, mutableRow, i)
      }
      if (value.isVector) {
        getters(i).apply(value.asVector().toString, mutableRow, i)
      }
      if (value.isDecimal) {
        getters(i).apply(value.asDecimal().toString, mutableRow, i)
      }
      if (value.isGeography) {
        getters(i).apply(value.asGeography().toString, mutableRow, i)
      }
    }
    LOG.info(s"mutable row info:$mutableRow")
    mutableRow
  }

  def closeGraphProvider(): Unit = graphProvider.close()

}

