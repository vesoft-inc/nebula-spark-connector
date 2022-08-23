/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

package com.vesoft.nebula.connector.writer

case class NebulaCommitMessage(partitionId: Int, executeStatements: List[String])
