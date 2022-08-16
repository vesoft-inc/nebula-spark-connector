/* Copyright (c) 2022 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

package com.vesoft.nebula.connector.reader

import org.apache.spark.sql.catalyst.InternalRow

/**
  * @todo
  * iterator for nebula vertex or edge data
  * convert each vertex data or edge data to Spark SQL's Row
  */
abstract class AbstractNebulaIterator extends Iterator[InternalRow] {

  /**
    * @todo
    * whether this iterator can provide another element.
    */
  override def hasNext: Boolean

  /**
    * @todo
    * Produces the next vertex or edge of this iterator.
    */
  override def next(): InternalRow
}
