/*
 * Copyright (c) 2025 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

package com.vesoft.nebula.spark.common

object DataTypeEnum extends Enumeration {

  type DataType = Value
  val NODE = Value("node")
  val EDGE = Value("edge")
  val GQL = Value("gql")

  def validDataType(dataType: String): Boolean =
    values.exists(_.toString.equalsIgnoreCase(dataType))
}

object OperaType extends Enumeration {

  type Operation = Value
  val READ = Value("read")
  val WRITE = Value("write")
}

object WriteMode extends Enumeration {

  type Mode = Value
  val INSERT = Value("insert")
  val INSERTREPLACE = Value("insert_replace")
  val INSERTIGNORE = Value("insert_ignore")
  val INSERTUPDATE = Value("insert_update")
  val UPDATE = Value("update")
  val DELETE = Value("delete")
  val DETACHDELETE=Value("detach_delete")
}
