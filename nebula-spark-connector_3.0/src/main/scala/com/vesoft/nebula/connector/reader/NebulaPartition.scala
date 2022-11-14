/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

package com.vesoft.nebula.connector.reader

import org.apache.spark.sql.connector.read.InputPartition

case class NebulaPartition(partition: Int) extends InputPartition
