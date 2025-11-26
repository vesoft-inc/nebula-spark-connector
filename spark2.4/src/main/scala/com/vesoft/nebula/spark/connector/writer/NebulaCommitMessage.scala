/*
 * Copyright (c) 2025 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

package com.vesoft.nebula.spark.connector.writer

import org.apache.spark.sql.sources.v2.writer.WriterCommitMessage

case class NebulaCommitMessage(executeStatements: List[String]) extends WriterCommitMessage
