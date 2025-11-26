/*
 * Copyright (c) 2025 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */
package com.vesoft.nebula.spark.common.reader

import com.vesoft.nebula.driver.graph.data.ValueWrapper
import com.vesoft.nebula.driver.graph.scan.{ScanEdgeResult, ScanEdgeResultIterator, ScanNodeResult, ScanNodeResultIterator, TableRow}
import com.vesoft.nebula.spark.common.NebulaUtils.NebulaValueGetter
import com.vesoft.nebula.spark.common.{NebulaOptions, NebulaUtils, PartitionUtils}
import com.vesoft.nebula.spark.common.nebula.GraphProvider
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.SpecificInternalRow
import org.apache.spark.sql.types.StructType
import org.slf4j.{Logger, LoggerFactory}

import java.time.format.DateTimeFormatter
import scala.collection.JavaConverters._
import scala.collection.mutable

trait NebulaReader {
  private val LOG: Logger = LoggerFactory.getLogger(this.getClass)

  private var schema: StructType = _

  protected var dataIterator    : Iterator[TableRow]               = _
  protected var scanPartIterator: Iterator[Int]                    = _
  protected var resultValues    : mutable.ListBuffer[List[Object]] = mutable.ListBuffer[List[Object]]()
  protected var graphProvider   : GraphProvider                    = _
  protected var nebulaOptions   : NebulaOptions                    = _

  private var nodeResponseIterator: ScanNodeResultIterator = _
  private var edgeResponseIterator: ScanEdgeResultIterator = _

  private val datetimeFormatter      = DateTimeFormatter.ISO_LOCAL_DATE_TIME
  private val zonedDatetimeFormatter = DateTimeFormatter.ISO_OFFSET_DATE_TIME
  private val timeFormatter          = DateTimeFormatter.ISO_LOCAL_TIME
  private val zonedTimeFormatter     = DateTimeFormatter.ISO_OFFSET_TIME

  /**
   * init the reader: init metaProvider, storageClient
   */
  def init(index: Int, nebulaOptions: NebulaOptions, schema: StructType): Unit = {
    this.schema = schema
    this.nebulaOptions = nebulaOptions

    // init graphProvider
    graphProvider = new GraphProvider(nebulaOptions)

    // allocate scanPart to this partition
    val totalPart = graphProvider.getAllParts.size()
    // index starts with 1
    val scanParts = PartitionUtils.getScanParts(index, totalPart, nebulaOptions.partitionNums)
    LOG.info(s"partition index: ${index}, scanParts: ${scanParts.toString}")
    scanPartIterator = scanParts.iterator
  }

  /**
   * resolve the node/edge data to InternalRow
   */
  protected def getRow(): InternalRow = {
    val resultSet: Array[ValueWrapper]      =
      dataIterator.next().getValues.toArray.map(v => v.asInstanceOf[ValueWrapper])
    val getters  : Array[NebulaValueGetter] = NebulaUtils.makeGetters(schema)
    val mutableRow                          = new SpecificInternalRow(schema.fields.map(x => x.dataType))

    // the value is property data, will not be Node,Edge,Record.
    for (i <- getters.indices) {
      val value: ValueWrapper = resultSet(i)
      if (value.isNull) {
        mutableRow.setNullAt(i)
      }
      if (value.isString) {
        getters(i).apply(value.asString(), mutableRow, i)
      }
      if (value.isDate) {
        getters(i).apply(value.asDate(), mutableRow, i)
      }
      if (value.isLocalTime) {
        getters(i).apply(value.asLocalTime().format(timeFormatter), mutableRow, i)
      }
      if (value.isZonedTime) {
        getters(i).apply(value.asZonedTime().format(zonedTimeFormatter), mutableRow, i)
      }
      if (value.isLocalDateTime) {
        getters(i).apply(value.asLocalDateTime().format(datetimeFormatter), mutableRow, i)
      }
      if (value.isZonedDateTime) {
        getters(i).apply(value.asZonedDateTime().format(zonedDatetimeFormatter), mutableRow, i)
      }
      if (value.isInt) {
        getters(i).apply(value.asInt(), mutableRow, i)
      }
      if (value.isLong) {
        getters(i).apply(value.asLong(), mutableRow, i)
      }
      if (value.isBoolean) {
        getters(i).apply(value.asBoolean(), mutableRow, i)
      }
      if (value.isFloat) {
        getters(i).apply(value.asFloat(), mutableRow, i)
      }
      if (value.isDouble) {
        getters(i).apply(value.asDouble(), mutableRow, i)
      }
      if (value.isDuration) {
        getters(i).apply(value.asDuration().toString, mutableRow, i)
      }
      if (value.isList) {
        getters(i).apply(value.asList().toString, mutableRow, i)
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
    mutableRow
  }

  /**
   * if the scan response has next node
   */
  protected def hasNextNodeRow: Boolean = {
    (dataIterator != null || nodeResponseIterator != null || scanPartIterator.hasNext) && {
      var continue: Boolean = false
      var break   : Boolean = false
      while ((dataIterator == null || !dataIterator.hasNext) && !break) {
        resultValues.clear()
        continue = false
        if (nodeResponseIterator == null || !nodeResponseIterator.hasNext) {
          if (scanPartIterator.hasNext) {
            try {
              // if returnCols is null, scan all the property of node, if returnCols is empty, just scan the pk of node.
              if (nebulaOptions.getReturnCols == null) {
                nodeResponseIterator = graphProvider.scanNode(nebulaOptions.schema,
                                                              nebulaOptions.graphName,
                                                              nebulaOptions.label,
                                                              scanPartIterator.next(),
                                                              nebulaOptions.batchSize)
              } else {
                nodeResponseIterator = graphProvider.scanNode(nebulaOptions.schema,
                                                              nebulaOptions.graphName,
                                                              nebulaOptions.label,
                                                              nebulaOptions.getReturnCols.asJava,
                                                              scanPartIterator.next(),
                                                              nebulaOptions.batchSize)
              }
            } catch {
              case e: Exception =>
                LOG.error(s"Exception scanning node type ${nebulaOptions.label}", e)
                graphProvider.close()
                throw new Exception(e.getMessage, e)
            }
            // jump to the next loop
            continue = true
          }
          // break while loop
          break = !continue
        } else {
          val next: ScanNodeResult = nodeResponseIterator.next
          if (!next.isEmpty) {
            dataIterator = next.getTableRows.iterator().asScala
          }
        }
      }

      dataIterator != null && dataIterator.hasNext
    }
  }

  /**
   * if the scan response has next edge
   */
  protected def hasNextEdgeRow: Boolean =
    (dataIterator != null || edgeResponseIterator != null || scanPartIterator.hasNext) && {
      var continue: Boolean = false
      var break   : Boolean = false
      while ((dataIterator == null || !dataIterator.hasNext) && !break) {
        resultValues.clear()
        continue = false
        if (edgeResponseIterator == null || !edgeResponseIterator.hasNext) {
          if (scanPartIterator.hasNext) {
            try {
              // if returnCols is null, scan src node pk and dst node pk and all the property of edge,
              // if returnCols is empty, just scan the pk of src node and dst node.
              if (nebulaOptions.getReturnCols == null) {
                edgeResponseIterator = graphProvider.scanEdge(nebulaOptions.schema,
                                                              nebulaOptions.graphName,
                                                              nebulaOptions.label,
                                                              scanPartIterator.next(),
                                                              nebulaOptions.batchSize)
              } else {
                edgeResponseIterator = graphProvider.scanEdge(nebulaOptions.schema,
                                                              nebulaOptions.graphName,
                                                              nebulaOptions.label,
                                                              nebulaOptions.getReturnCols.asJava,
                                                              scanPartIterator.next(),
                                                              nebulaOptions.batchSize)
              }
            } catch {
              case e: Exception =>
                LOG.error(s"Exception scanning edge type ${nebulaOptions.label}", e)
                graphProvider.close()
                throw new Exception(e.getMessage, e)
            }
            // jump to the next loop
            continue = true
          }
          // break while loop
          break = !continue
        } else {
          val next: ScanEdgeResult = edgeResponseIterator.next
          if (!next.isEmpty) {
            dataIterator = next.getTableRows.iterator().asScala
          }
        }
      }

      dataIterator != null && dataIterator.hasNext
    }

  /**
   * close the reader
   */
  protected def closeReader(): Unit = {
    graphProvider.close()
  }
}
