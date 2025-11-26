/*
 * Copyright (c) 2025 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */
package com.vesoft.nebula.spark.connector.reader

import com.vesoft.nebula.spark.common.{NebulaOptions}
import com.vesoft.nebula.spark.common.reader.NebulaGqlReader
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.sources.v2.reader.InputPartitionReader

class NebulaGqlPartitionReader extends InputPartitionReader[InternalRow] with NebulaGqlReader{
  def this(nebulaOptions: NebulaOptions) {
    this()
    init(nebulaOptions)
  }

  override def next(): Boolean = hasNext()

  override def get(): InternalRow = getRow()

  override def close(): Unit = closeGraphProvider()

}

