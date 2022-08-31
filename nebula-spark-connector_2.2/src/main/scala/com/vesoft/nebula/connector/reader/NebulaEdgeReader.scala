/* Copyright (c) 2022 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

package com.vesoft.nebula.connector.reader

import com.vesoft.nebula.client.storage.scan.{ScanEdgeResult, ScanEdgeResultIterator}
import com.vesoft.nebula.connector.NebulaOptions
import org.apache.spark.Partition
import org.apache.spark.sql.types.StructType
import org.slf4j.LoggerFactory
import scala.collection.JavaConverters._

class NebulaEdgeReader(split: Partition, nebulaOptions: NebulaOptions, schema: StructType)
    extends NebulaIterator(split, nebulaOptions, schema) {
  private val LOG = LoggerFactory.getLogger(this.getClass)

  private var responseIterator: ScanEdgeResultIterator = _

  override def hasNext: Boolean = {
    if (dataIterator == null && responseIterator == null && !scanPartIterator.hasNext)
      return false

    var continue: Boolean = false
    var break: Boolean    = false
    while ((dataIterator == null || !dataIterator.hasNext) && !break) {
      resultValues.clear()
      continue = false
      if (responseIterator == null || !responseIterator.hasNext) {
        if (scanPartIterator.hasNext) {
          try {
            if (nebulaOptions.noColumn) {
              responseIterator = storageClient.scanEdge(nebulaOptions.spaceName,
                                                        scanPartIterator.next(),
                                                        nebulaOptions.label,
                                                        nebulaOptions.limit,
                                                        0L,
                                                        Long.MaxValue,
                                                        true,
                                                        true)
            } else {
              responseIterator = storageClient.scanEdge(nebulaOptions.spaceName,
                                                        scanPartIterator.next(),
                                                        nebulaOptions.label,
                                                        nebulaOptions.getReturnCols.asJava,
                                                        nebulaOptions.limit,
                                                        0,
                                                        Long.MaxValue,
                                                        true,
                                                        true)
            }
          } catch {
            case e: Exception =>
              LOG.error(s"Exception scanning vertex ${nebulaOptions.label}", e)
              storageClient.close()
              throw new Exception(e.getMessage, e)
          }
          // jump to the next loop
          continue = true
        }
        // break while loop
        break = !continue
      } else {
        val next: ScanEdgeResult = responseIterator.next
        if (!next.isEmpty) {
          dataIterator = next.getEdgeTableRows.iterator().asScala
        }
      }
    }

    if (dataIterator == null) {
      return false
    }
    dataIterator.hasNext
  }
}
