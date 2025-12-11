/*
 * Copyright (c) 2025 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

package com.vesoft.nebula.spark.common.nebula

import scala.collection.mutable.ArrayBuffer


case class NodeDesc(nodeTypeName: String,
                    nodePkNames: List[String],
                    propNames: ArrayBuffer[String],
                    properties: Map[String, String])

case class EdgeDesc(edgeTypeName: String,
                    isDirected: Boolean,
                    srcNodeTypeName: String,
                    srcNodePkNames: List[String],
                    srcNodePkDataTypeMap: Map[String, String],
                    dstNodeTypeName: String,
                    dstNodePkNames: List[String],
                    dstNodePkDataTypeMap: Map[String, String],
                    propNames: ArrayBuffer[String],
                    properties: Map[String, String])
