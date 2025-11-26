/*
 * Copyright (c) 2025 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */
package com.vesoft.nebula.spark.connector.mock

import com.vesoft.nebula.driver.graph.net.NebulaClient
import com.vesoft.nebula.spark.connector.TestServerInfo
import org.apache.log4j.Logger

class NebulaGraphMock {
  private[this] val LOG = Logger.getLogger(this.getClass)

  val graphAddr = TestServerInfo.host
  val user      = TestServerInfo.user
  val passwd    = TestServerInfo.passwd
  @transient val client: NebulaClient = NebulaClient.builder(graphAddr, user, passwd).build()

  def mockReadGraph(): Unit = {
    try {
      val createGraphType = "CREATE GRAPH TYPE IF NOT EXISTS spark3_read_type AS {" +
        "NODE TYPE node_player (LABEL player {col1 INT PRIMARY KEY, col2 STRING, col3 FLOAT, col4 bool, col5 DOUBLE, col6 INT64, col7 local time, col8 local datetime, col9 zoned time})," +
        "EDGE TYPE edge_follow (node_player)-[LABEL follow {ecol1 INT, ecol2 FLOAT64, ecol3 STRING, ecol4 DOUBLE}]->(node_player)}"

      var result = client.execute(createGraphType)
      if (!result.isSucceeded) {
        LOG.error(s"create graph type spark_read_type failed: ${result.getErrorMessage}")
        System.exit(1)
      }
      Thread.sleep(3000)


      val createGraph = "CREATE GRAPH IF NOT EXISTS spark3_read spark3_read_type"
      result = client.execute(createGraph)
      if (!result.isSucceeded) {
        LOG.error("create graph spark_read failed: $result.getErrorMessage")
        System.exit(1)
      }
      Thread.sleep(3000)

      val insertNode = "TABLE t{col1,col2,col3,col4,col5,col6,col7,col8,col9}=" +
        "(1, \"Tim\", 11.0, true, 7.99, 10, local_time(\"12:00:00\"), local_datetime(\"2024-01-01T12:00:00.123\"),  zoned_time(\"12:00:00.000+0800\"))," +
        "(2, \"Tom\", 12.0, false, 8.18, 11, local_time(\"13:00:00.222\"), local_datetime(\"2024-01-01T12:00:00\"),  zoned_time(\"12:00:00.000+0830\"))," +
        "(3, \"Bob\",  13.1, true, 9.76, 12, local_time(\"10:10:20.123\"), local_datetime(\"2024-01-01T12:00:00\"),  zoned_time(\"12:00:00.000+0200\"))," +
        "(4, \"Jena\",  14.5, true, 0.55, 13, local_time(\"12:00:01.999\"), local_datetime(\"2024-01-01T12:00:00\"),  zoned_time(\"12:00:00.000+0230\"))," +
        "(5, \"Alex\", 15.6, true, 1.42, 14, local_time(\"12:00:00\"), local_datetime(\"2024-01-01T12:00:00\"),  zoned_time(\"12:00:00.000+0900\"))," +
        "(6, \"James\", 16.7, true, 2.34, 15, local_time(\"12:00:00\"), local_datetime(\"2024-01-01T12:00:00\"),  zoned_time(\"12:00:00.000+0900\"))," +
        "(7, \"Emma\", 17.8, true, 3.65, 16, local_time(\"12:00:00\"), local_datetime(\"2024-01-01T12:00:00\"),  zoned_time(\"12:00:00.000+0800\"))," +
        "(8, \"Michael\", 18.1, true, 4.12, 17, local_time(\"12:00:00\"), local_datetime(\"2024-01-01T12:00:00\"),  zoned_time(\"12:00:00.000+0000\"))," +
        "(9, \"Sarah\", 19.2, false, 15.01, 18, local_time(\"12:00:00\"), local_datetime(\"2024-01-01T12:00:00\"),  zoned_time(\"12:00:00.000+0800\"))," +
        "(10, \"Jessica\", 20.6, false, 21.56, 19, local_time(\"12:00:00\"), local_datetime(\"2024-01-01T12:00:00\"),  zoned_time(\"12:00:00.000+0800\"))," +
        "(-1, \"William\", 21.5, false, 45.43, 20, null, local_datetime(\"2024-01-01T12:00:00\"),  zoned_time(\"12:00:00.000+0800\"))," +
        "(-2, \"Robert\",  22.8, false, 21.43, 21, local_time(\"12:00:00\"), local_datetime(\"2024-01-01T12:00:00\"),  zoned_time(\"12:00:00.000+0800\"))," +
        "(-3, \"David\", 23.9, false, 72.15, 22, local_time(\"12:00:00\"), local_datetime(\"2024-01-01T12:00:00\"),  zoned_time(\"12:00:00.000+0800\"))" +
        "use spark3_read for r in t \n" +
        "insert or ignore (@node_player{col1:r.col1,col2:r.col2,col3:r.col3,col4:r.col4,col5:r.col5,col6:r.col6,col7:r.col7,col8:r.col8,col9:r.col9})"

      result = client.execute(insertNode)
      if (!result.isSucceeded) {
        LOG.error(s"insert node for graph spark3_read failed: ${result.getErrorMessage}")
        System.exit(1)
      }

      val insertEdge = "TABLE t{src,dst,col1,col2,col3,col4}=" +
        "(1,2,90,66.8,\"A\",1.0)," +
        "(2,3,90,66.8,\"B\",1.0)," +
        "(4,5,90,66.8,\"C\",1.0)," +
        "(5,1,90,66.8,\"D\",1.0)," +
        "(6,8,90,66.8,\"E\",1.0)," +
        "(7,9,90,66.8,\"F\",1.0)," +
        "(8,10,90,66.8,\"G\",1.0)," +
        "(9,-1,90,66.8,\"H\",1.0)," +
        "(-1,-1,90,66.8,null,1.0)," +
        "(-3,1,90,66.8,\"\",1.0)" +
        "use spark3_read \n" +
        "for r in t return r.src as src,r.dst as dst,r.col1 as col1,r.col2 as col2,r.col3 as col3,r.col4 as col4\n" +
        "next\n" +
        "use spark3_read\n" +
        "optional match(src_v@node_player) where src_v.col1=src \n" +
        "optional match(dst_v@node_player) where dst_v.col1=dst \n" +
        "insert or ignore (src_v)-[e@edge_follow{ecol1:col1,ecol2:col2,ecol3:col3,ecol4:col4}]->(dst_v)"
      result = client.execute(insertEdge)
      if (!result.isSucceeded) {
        LOG.error("insert edge for graph {} failed: {}", "spark3_read", result.getErrorMessage)
        System.exit(1)
      }
    } catch {
      case e: Exception => LOG.error("mock read graph failed", e)
        System.exit(1)
    } finally {
      client.close()
    }
  }

}
