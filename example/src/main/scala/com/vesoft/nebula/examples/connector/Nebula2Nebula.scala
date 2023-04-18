/* Copyright (c) 2023 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

package com.vesoft.nebula.examples.connector

import com.facebook.thrift.protocol.TCompactProtocol
import com.vesoft.nebula.client.meta.MetaClient
import com.vesoft.nebula.connector.{
  NebulaConnectionConfig,
  ReadNebulaConfig,
  WriteNebulaEdgeConfig,
  WriteNebulaVertexConfig
}
import com.vesoft.nebula.connector.connector.NebulaDataFrameReader
import com.vesoft.nebula.connector.connector.NebulaDataFrameWriter
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.commons.cli.{
  CommandLine,
  CommandLineParser,
  DefaultParser,
  HelpFormatter,
  Option,
  Options,
  ParseException
}

import scala.collection.JavaConverters.asScalaBufferConverter
import scala.collection.mutable.ListBuffer

/**
  * spark-submit --master local \
  * --conf spark.driver.extraClassPath=./ \
  * --conf spark.executor.extraClassPath=./  \
  * --jars commons-cli-1.4.jar \
  * --class com.vesoft.nebula.examples.connector.Nebula2Nebula example-3.0-SNAPSHOT-jar-with-dependencies.jar \
  * -sourceMeta "192.168.8.171:9559" -sourceSpace "source"  -limit 2 -targetMeta "192.168.8.171:9559" -targetGraph "192.168.8.171:9669" -targetSpace "target" -batch 2 -timeout 50000 -u root -passwd nebula
  *
  */
object Nebula2Nebula {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf
    sparkConf
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .registerKryoClasses(Array[Class[_]](classOf[TCompactProtocol]))
    val spark = SparkSession
      .builder()
      .config(sparkConf)
      .getOrCreate()

    // args：source metad address,source space name,data limit for one reading request;
    // target metad address,target graphd address,target space name, batch size for on writing request

    val sourceMetaOption =
      new Option("sourceMeta", "sourceMetaAddress", true, "source nebulagraph metad address")
    sourceMetaOption.setRequired(true)
    val sourceSpaceOption =
      new Option("sourceSpace", "sourceSpace", true, "source nebulagraph space name")
    sourceSpaceOption.setRequired(true)
    val limitOption =
      new Option("limit", "limit", true, "records for one reading request for reading")
    limitOption.setRequired(true)
    val targetMetaOption =
      new Option("targetMeta", "targetMetaAddress", true, "target nebulagraph metad address")
    targetMetaOption.setRequired(true)
    val targetGraphOption =
      new Option("targetGraph", "targetGraphAddress", true, "target nebulagraph graphd address")
    targetGraphOption.setRequired(true)
    val targetSpaceOption =
      new Option("targetSpace", "targetSpace", true, "target nebulagraph space name")
    targetSpaceOption.setRequired(true)
    val batchOption = new Option("batch", "batch", true, "batch size for one insert request")
    batchOption.setRequired(true)
    val writeParallelOption = new Option("p", "parallel", true, "parallel for writing data")
    writeParallelOption.setRequired(true)
    val timeoutOption = new Option("timeout", "timeout", true, "timeout for java client");
    timeoutOption.setRequired(true)
    val userOption = new Option("u", "user", true, "user")
    userOption.setRequired(true)
    val passwdOption = new Option("passwd", "password", true, "password")
    passwdOption.setRequired(true)

    // filter out some tags /edges
    val missTagsOption =
      new Option("misstags", "misstags", true, "filter out these tags, separate with `,`")
    val missEdgesOption =
      new Option("missedges", "missedges", true, "filter out these edges, separate with `,`")

    val options = new Options
    options.addOption(sourceMetaOption)
    options.addOption(sourceSpaceOption)
    options.addOption(limitOption)
    options.addOption(targetMetaOption)
    options.addOption(targetGraphOption)
    options.addOption(targetSpaceOption)
    options.addOption(batchOption)
    options.addOption(writeParallelOption)
    options.addOption(timeoutOption)
    options.addOption(userOption)
    options.addOption(passwdOption)
    options.addOption(missTagsOption)
    options.addOption(missEdgesOption)

    var cli: CommandLine             = null
    val cliParser: CommandLineParser = new DefaultParser
    val helpFormatter                = new HelpFormatter

    try {
      cli = cliParser.parse(options, args)
    } catch {
      case e: ParseException =>
        helpFormatter.printHelp(">>>> options", options)
        e.printStackTrace()
        System.exit(1)
    }

    val sourceMetaAddr: String  = cli.getOptionValue("sourceMeta")
    val sourceSpace: String     = cli.getOptionValue("sourceSpace")
    val limit: Int              = cli.getOptionValue("limit").toInt
    val targetMetaAddr: String  = cli.getOptionValue("targetMeta")
    val targetGraphAddr: String = cli.getOptionValue("targetGraph")
    val targetSpace: String     = cli.getOptionValue("targetSpace")
    val batch: Int              = cli.getOptionValue("batch").toInt
    val parallel: Int           = cli.getOptionValue("p").toInt
    val timeout: Int            = cli.getOptionValue("timeout").toInt
    val user: String            = cli.getOptionValue("u")
    val passed: String          = cli.getOptionValue("passwd")
    val misstags: List[String] =
      if (cli.hasOption("misstags")) cli.getOptionValue("misstags").split(",").toList else List()
    val missedges: List[String] =
      if (cli.hasOption("missedges")) cli.getOptionValue("missedges").split(",").toList else List()

    // common config
    val sourceConnectConfig =
      NebulaConnectionConfig
        .builder()
        .withMetaAddress(sourceMetaAddr)
        .withConenctionRetry(2)
        .withTimeout(timeout)
        .build()

    val targetConnectConfig =
      NebulaConnectionConfig
        .builder()
        .withMetaAddress(targetMetaAddr)
        .withGraphAddress(targetGraphAddr)
        .withTimeout(timeout)
        .withConenctionRetry(2)
        .build()

    val metaHostAndPort = sourceMetaAddr.split(":")
    var (tags, edges, partitions) =
      getTagsAndEdges(metaHostAndPort(0), metaHostAndPort(1).toInt, sourceSpace)

    if (misstags.nonEmpty) {
      tags = tags.dropWhile(ele => misstags.contains(ele))
    }
    if (missedges.nonEmpty) {
      edges = edges.dropWhile(ele => missedges.contains(ele))
    }

    tags.foreach(tag => {
      syncTag(spark,
              sourceConnectConfig,
              sourceSpace,
              limit,
              partitions,
              targetConnectConfig,
              targetSpace,
              batch,
              tag,
              parallel,
              user,
              passed)
    })

    edges.foreach(edge => {
      syncEdge(spark,
               sourceConnectConfig,
               sourceSpace,
               limit,
               partitions,
               targetConnectConfig,
               targetSpace,
               batch,
               edge,
               parallel,
               user,
               passed)
    })
  }

  def getTagsAndEdges(metaHost: String,
                      metaPort: Int,
                      space: String): (List[String], List[String], Int) = {
    val metaClient: MetaClient = new MetaClient(metaHost, metaPort)
    metaClient.connect()
    val tags: ListBuffer[String]  = new ListBuffer[String]
    val edges: ListBuffer[String] = new ListBuffer[String]

    for (tag <- (metaClient.getTags(space)).asScala) {
      tags.append(new String(tag.tag_name))
    }

    for (edge <- (metaClient.getEdges(space).asScala)) {
      edges.append(new String(edge.edge_name))
    }

    val partitions = metaClient.getPartsAlloc(space).size()
    (tags.toList, edges.toList, partitions)
  }

  def syncTag(spark: SparkSession,
              sourceConfig: NebulaConnectionConfig,
              sourceSpace: String,
              limit: Int,
              readPartition: Int,
              targetConfig: NebulaConnectionConfig,
              targetSpace: String,
              batch: Int,
              tag: String,
              writeParallel: Int,
              user: String,
              passwd: String): Unit = {
    val nebulaReadVertexConfig: ReadNebulaConfig = ReadNebulaConfig
      .builder()
      .withSpace(sourceSpace)
      .withLabel(tag)
      .withReturnCols(List())
      .withLimit(limit)
      .withPartitionNum(readPartition)
      .build()
    var vertex = spark.read.nebula(sourceConfig, nebulaReadVertexConfig).loadVerticesToDF()

    if (readPartition != writeParallel) {
      vertex = vertex.repartition(writeParallel)
    }

    val nebulaWriteVertexConfig: WriteNebulaVertexConfig = WriteNebulaVertexConfig
      .builder()
      .withSpace(targetSpace)
      .withUser(user)
      .withPasswd(passwd)
      .withTag(tag)
      .withVidField("_vertexId")
      .withBatch(batch)
      .build()
    vertex.write.nebula(targetConfig, nebulaWriteVertexConfig).writeVertices()

  }

  def syncEdge(spark: SparkSession,
               sourceConfig: NebulaConnectionConfig,
               sourceSpace: String,
               limit: Int,
               readPartition: Int,
               targetConfig: NebulaConnectionConfig,
               targetSpace: String,
               batch: Int,
               edge: String,
               writeParallel: Int,
               user: String,
               passwd: String): Unit = {
    val nebulaReadEdgeConfig: ReadNebulaConfig = ReadNebulaConfig
      .builder()
      .withSpace(sourceSpace)
      .withLabel(edge)
      .withReturnCols(List())
      .withLimit(limit)
      .withPartitionNum(readPartition)
      .build()
    var edgeDf = spark.read.nebula(sourceConfig, nebulaReadEdgeConfig).loadEdgesToDF()

    if (readPartition != writeParallel) {
      edgeDf = edgeDf.repartition(writeParallel)
    }

    val nebulaWriteEdgeConfig: WriteNebulaEdgeConfig = WriteNebulaEdgeConfig
      .builder()
      .withSpace(targetSpace)
      .withUser(user)
      .withPasswd(passwd)
      .withEdge(edge)
      .withSrcIdField("_srcId")
      .withDstIdField("_dstId")
      .withRankField("_rank")
      .withBatch(batch)
      .build()
    edgeDf.write.nebula(targetConfig, nebulaWriteEdgeConfig).writeEdges()
  }

}
