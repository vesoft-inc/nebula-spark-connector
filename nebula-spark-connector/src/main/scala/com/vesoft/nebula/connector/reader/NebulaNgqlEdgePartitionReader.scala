/* Copyright (c) 2022 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

package com.vesoft.nebula.connector.reader

import com.vesoft.nebula.Value
import com.vesoft.nebula.client.graph.data.{Relationship, ResultSet, ValueWrapper}
import com.vesoft.nebula.connector.NebulaUtils.NebulaValueGetter
import com.vesoft.nebula.connector.nebula.GraphProvider
import com.vesoft.nebula.connector.{NebulaOptions, NebulaUtils}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.SpecificInternalRow
import org.apache.spark.sql.sources.v2.reader.InputPartitionReader
import org.apache.spark.sql.types.StructType
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.JavaConversions.asScalaBuffer
import scala.collection.mutable
import scala.collection.mutable.ListBuffer

/**
  * create reader by ngql
  */
class NebulaNgqlEdgePartitionReader extends InputPartitionReader[InternalRow] {

  private val LOG: Logger = LoggerFactory.getLogger(this.getClass)

  private var nebulaOptions: NebulaOptions                     = _
  private var graphProvider: GraphProvider                     = _
  private var schema: StructType                               = _
  private var resultSet: ResultSet                             = _
  private var edgeIterator: Iterator[ListBuffer[ValueWrapper]] = _

  def this(nebulaOptions: NebulaOptions, schema: StructType) {
    this()
    this.schema = schema
    this.nebulaOptions = nebulaOptions
    this.graphProvider = new GraphProvider(
      nebulaOptions.getGraphAddress,
      nebulaOptions.user,
      nebulaOptions.passwd,
      nebulaOptions.timeout,
      nebulaOptions.enableGraphSSL,
      nebulaOptions.sslSignType,
      nebulaOptions.caSignParam,
      nebulaOptions.selfSignParam,
      nebulaOptions.version
    )
    // add exception when session build failed
    graphProvider.switchSpace(nebulaOptions.spaceName)
    resultSet = graphProvider.submit(nebulaOptions.ngql)
    edgeIterator = query()
  }

  def query(): Iterator[ListBuffer[ValueWrapper]] = {
    val edges: ListBuffer[ListBuffer[ValueWrapper]] = new ListBuffer[ListBuffer[ValueWrapper]]
    val properties                                  = nebulaOptions.getReturnCols
    for (i <- 0 until resultSet.rowsSize()) {
      val rowValues = resultSet.rowValues(i).values()
      for (j <- 0 until rowValues.size()) {
        val value     = rowValues.get(j)
        val valueType = value.getValue.getSetField
        if (valueType == Value.EVAL) {
          val relationship = value.asRelationship()
          if (checkLabel(relationship)) {
            edges.append(convertToEdge(relationship, properties))
          }
        } else if (valueType == Value.LVAL) {
          val list: mutable.Buffer[ValueWrapper] = value.asList()
          edges.appendAll(
            list.toStream
              .filter(e => e != null && e.isEdge() && checkLabel(e.asRelationship()))
              .map(e => convertToEdge(e.asRelationship(), properties))
          )
        } else if (valueType == Value.PVAL){
          val list: java.util.List[Relationship] = value.asPath().getRelationships()
          edges.appendAll(
            list.toStream
              .filter(e => checkLabel(e))
              .map(e => convertToEdge(e, properties))
          )
        } else if (valueType != Value.NVAL && valueType != 0) {
          LOG.error(s"Unexpected edge type encountered: ${valueType}. Only edge or path should be returned.")
          throw new RuntimeException("Invalid nGQL return type. Value type conversion failed.");
        }
      }
    }
    edges.iterator
  }

  def checkLabel(relationship: Relationship): Boolean = {
    this.nebulaOptions.label.equals(relationship.edgeName())
  }

  def convertToEdge(relationship: Relationship,
                    properties: List[String]): ListBuffer[ValueWrapper] = {
    val edge: ListBuffer[ValueWrapper] = new ListBuffer[ValueWrapper]
    edge.append(relationship.srcId())
    edge.append(relationship.dstId())
    edge.append(new ValueWrapper(new Value(Value.IVAL, relationship.ranking()), "utf-8"))
    if (properties == null || properties.isEmpty)
      return edge
    else {
      for (i <- properties.indices) {
        edge.append(relationship.properties().get(properties(i)))
      }
    }
    edge
  }

  override def next(): Boolean = {
    edgeIterator.hasNext
  }

  override def get(): InternalRow = {
    val getters: Array[NebulaValueGetter] = NebulaUtils.makeGetters(schema)
    val mutableRow                        = new SpecificInternalRow(schema.fields.map(x => x.dataType))

    val edge = edgeIterator.next();
    for (i <- getters.indices) {
      val value: ValueWrapper = edge(i)
      var resolved            = false
      if (value.isNull) {
        mutableRow.setNullAt(i)
        resolved = true
      }
      if (value.isString) {
        getters(i).apply(value.asString(), mutableRow, i)
        resolved = true
      }
      if (value.isDate) {
        getters(i).apply(value.asDate(), mutableRow, i)
        resolved = true
      }
      if (value.isTime) {
        getters(i).apply(value.asTime(), mutableRow, i)
        resolved = true
      }
      if (value.isDateTime) {
        getters(i).apply(value.asDateTime(), mutableRow, i)
        resolved = true
      }
      if (value.isLong) {
        getters(i).apply(value.asLong(), mutableRow, i)
      }
      if (value.isBoolean) {
        getters(i).apply(value.asBoolean(), mutableRow, i)
      }
      if (value.isDouble) {
        getters(i).apply(value.asDouble(), mutableRow, i)
      }
      if (value.isGeography) {
        getters(i).apply(value.asGeography(), mutableRow, i)
      }
      if (value.isDuration) {
        getters(i).apply(value.asDuration(), mutableRow, i)
      }
    }
    mutableRow

  }

  override def close(): Unit = {
    graphProvider.close();
  }
}
