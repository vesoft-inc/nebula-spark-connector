/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

package com.vesoft.nebula.connector.writer

import org.apache.spark.sql.connector.write.WriterCommitMessage

case class NebulaCommitMessage(executeStatements: List[String]) extends WriterCommitMessage
