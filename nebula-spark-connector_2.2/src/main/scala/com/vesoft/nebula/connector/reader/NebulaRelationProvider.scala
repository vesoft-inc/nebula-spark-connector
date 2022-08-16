/* Copyright (c) 2022 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

package com.vesoft.nebula.connector.reader

import com.vesoft.nebula.connector.{NebulaOptions, OperaType}
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.sources.{BaseRelation, DataSourceRegister, RelationProvider}

class NebulaRelationProvider extends RelationProvider with DataSourceRegister {

  /**
    * The string that represents the format that nebula data source provider uses.
    */
  override def shortName(): String = "nebula"

  /**
    * Returns a new base relation with the given parameters.
    * you can see it as reader.
    */
  override def createRelation(sqlContext: SQLContext,
                              parameters: Map[String, String]): BaseRelation = {
    val nebulaOptions = new NebulaOptions(parameters, OperaType.READ)
    NebulaRelation(sqlContext, nebulaOptions)
  }
}
