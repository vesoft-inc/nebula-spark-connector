/* Copyright (c) 2023 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

package com.vesoft.nebula.examples.connector

import com.facebook.thrift.protocol.TCompactProtocol
import com.vesoft.nebula.connector.connector.NebulaDataFrameReader
import com.vesoft.nebula.connector.connector.NebulaDataFrameWriter
import com.vesoft.nebula.connector.{NebulaConnectionConfig, ReadNebulaConfig, WriteNebulaEdgeConfig}
import org.apache.commons.cli.{
  CommandLine,
  CommandLineParser,
  HelpFormatter,
  Option,
  Options,
  ParseException,
  PosixParser
}
import org.apache.spark.SparkConf
import org.apache.spark.sql.functions.{col, sum}
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.collection.mutable.ListBuffer

object AggregateData {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf
    sparkConf
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .registerKryoClasses(Array[Class[_]](classOf[TCompactProtocol]))
    val spark = SparkSession
      .builder()
      .master("local[*]")
      .config(sparkConf)
      .getOrCreate()

    val metaAddressOption =
      new Option("m", "metaAddress", true, "NebulaGraph metad address for scanning data")
    metaAddressOption.setRequired(true)
    val spaceOption =
      new Option("space", "space", true, "NebulaGraph space name for scanning data")
    spaceOption.setRequired(true)
    val limitOption =
      new Option("l", "limit", true, "records amount for one reading request for reading")
    limitOption.setRequired(true)
    val partitionOption =
      new Option(
        "p",
        "partition",
        true,
        "partition for spark while scanning data, upper limit is nebula space partition_num")
    partitionOption.setRequired(true)
    val tagOption =
      new Option("t", "tag", true, "NebulaGraph tag name for scanning data")
    tagOption.setRequired(true)
    val edgeOption =
      new Option("e", "edge", true, "NebulaGraph edge type for scanning data")
    edgeOption.setRequired(true)
    val tagPropsOption = new Option("tp", "tagprops", true, "tag props to scan out, split by comma")
    tagPropsOption.setRequired(true)
    val edgePropsOption =
      new Option("ep", "edgeprops", true, "edge props to scan out, split by comma")
    edgePropsOption.setRequired(true)
    val timeoutOption =
      new Option("timeout", "timeout", true, "timeout for request, used for writing result");
    timeoutOption.setRequired(true)
    val userOption =
      new Option("u", "user", true, "target NebulaGraph user, used for writing result")
    userOption.setRequired(true)
    val passwdOption =
      new Option("passwd", "password", true, "target NebulaGraph password, used for writing result")
    passwdOption.setRequired(true)
    val edgeAggregateFiledsOption =
      new Option("efields",
                 "edgeAggregateFields",
                 true,
                 "aggregate fields in edge, used by join condition with tag data, split by comma")
    edgeAggregateFiledsOption.setRequired(true)
    val tagAggregateFiledOption =
      new Option(
        "tfield",
        "tagAggregateField",
        true,
        "aggregate field in vertex, \"_vertexId\" for vid, used by join condition with edge data")
    tagAggregateFiledOption.setRequired(true)
    val reduceFieldOption =
      new Option("reduceField", "reduceField", true, "reduce field for edge, such as 金额")
    reduceFieldOption.setRequired(true)
    val tagClassFieldOption =
      new Option("tagTypeField", "tagTypeField", true, "tag prop name defined tag's type")
    tagClassFieldOption.setRequired(true)
    val targetSpaceOption =
      new Option("targetSpace",
                 "targetSpace",
                 true,
                 "target space used for writing aggregate result")
    targetSpaceOption.setRequired(true)
    val targetEdgeTypeOption =
      new Option("targetEdge", "targetEdge", true, "target edge type used for writing result")
    targetEdgeTypeOption.setRequired(true)
    val targetGraphAddressOption =
      new Option("targetGraphAddr",
                 "targetGraphAddr",
                 true,
                 "target graph address used for writing result, split by comma")
    targetGraphAddressOption.setRequired(true)
    val targetMetaAddressOption =
      new Option("targetMetaAddr",
                 "targetMetaAddr",
                 true,
                 "target meta address used for writing result")
    targetMetaAddressOption.setRequired(true)
    val targetEdgePropFieldOption = new Option("targetEdgePropField",
                                               "targetEdgePropField",
                                               true,
                                               "target edge prop field to save the count of amount")
    targetEdgePropFieldOption.setRequired(true)
    val batchOption =
      new Option("b", "batch", true, "batch size to write aggregate result into edge")
    batchOption.setRequired(true)

    val options = new Options
    options.addOption(metaAddressOption)
    options.addOption(spaceOption)
    options.addOption(limitOption)
    options.addOption(partitionOption)
    options.addOption(tagOption)
    options.addOption(edgeOption)
    options.addOption(tagPropsOption)
    options.addOption(edgePropsOption)
    options.addOption(timeoutOption)
    options.addOption(userOption)
    options.addOption(passwdOption)
    options.addOption(edgeAggregateFiledsOption)
    options.addOption(tagAggregateFiledOption)
    options.addOption(reduceFieldOption)
    options.addOption(tagClassFieldOption)
    options.addOption(targetSpaceOption)
    options.addOption(targetEdgeTypeOption)
    options.addOption(targetGraphAddressOption)
    options.addOption(targetMetaAddressOption)
    options.addOption(targetEdgePropFieldOption)
    options.addOption(batchOption)

    var cli: CommandLine             = null
    val cliParser: CommandLineParser = new PosixParser()
    val helpFormatter                = new HelpFormatter

    try {
      cli = cliParser.parse(options, args)
    } catch {
      case e: ParseException =>
        helpFormatter.printHelp(">>>> options", options)
        e.printStackTrace()
        System.exit(1)
    }

    val metaAddr: String            = cli.getOptionValue("metaAddress")
    val space: String               = cli.getOptionValue("space")
    val limit: Int                  = cli.getOptionValue("limit").toInt
    val tag: String                 = cli.getOptionValue("t")
    val edgeType: String            = cli.getOptionValue("e")
    val tagProps: List[String]      = cli.getOptionValue("tp").split(",").toList
    val edgeProps: List[String]     = cli.getOptionValue("ep").split(",").toList
    val partition: Int              = cli.getOptionValue("p").toInt
    val timeout: Int                = cli.getOptionValue("timeout").toInt
    val user: String                = cli.getOptionValue("u")
    val passed: String              = cli.getOptionValue("passwd")
    val edgeAggFields: List[String] = cli.getOptionValue("efields").split(",").toList
    val tagAggField: String         = cli.getOptionValue("tfield")
    val reduceField: String         = cli.getOptionValue("reduceField")
    val tagTypeField: String        = cli.getOptionValue("tagTypeField")
    val targetSpace: String         = cli.getOptionValue("targetSpace")
    val targetEdge: String          = cli.getOptionValue("targetEdge")
    val targetGraphAddr: String     = cli.getOptionValue("targetGraphAddr")
    val targetMetaAddr: String      = cli.getOptionValue("targetMetaAddr")
    val targetEdgeProp: String      = cli.getOptionValue("targetEdgePropField")
    val batch: Int                  = cli.getOptionValue("b").toInt

    // read edge data
    val edgeDF = readEdges(spark, metaAddr, space, edgeType, edgeProps, limit, partition)
    // read tag data
    val tagDF = readVertex(spark, metaAddr, space, tag, tagProps, limit, partition)
    // aggregate
    val result =
      aggregateData(edgeDF, tagDF, edgeProps, tagTypeField, edgeAggFields, tagAggField, reduceField)
    // write result into NebulaGraph
    writeResult(result,
                targetGraphAddr,
                targetMetaAddr,
                targetSpace,
                targetEdge,
                targetEdgeProp,
                batch,
                user,
                passed,
                timeout)
  }

  def readVertex(spark: SparkSession,
                 metaAddress: String,
                 space: String,
                 tag: String,
                 props: List[String],
                 limit: Int,
                 partition: Int): DataFrame = {
    val config =
      NebulaConnectionConfig
        .builder()
        .withMetaAddress(metaAddress)
        .withConenctionRetry(2)
        .build()
    val nebulaReadVertexConfig: ReadNebulaConfig = ReadNebulaConfig
      .builder()
      .withSpace(space)
      .withLabel(tag)
      .withNoColumn(false)
      .withReturnCols(props)
      .withLimit(limit)
      .withPartitionNum(partition)
      .build()
    val vertex = spark.read.nebula(config, nebulaReadVertexConfig).loadVerticesToDF()
    vertex.printSchema()
    vertex.show(20)
    println("vertex count: " + vertex.count())
    vertex
  }

  def readEdges(spark: SparkSession,
                metaAddress: String,
                space: String,
                edgeType: String,
                props: List[String],
                limit: Int,
                partition: Int): DataFrame = {
    val config =
      NebulaConnectionConfig
        .builder()
        .withMetaAddress(metaAddress)
        .withTimeout(6000)
        .withConenctionRetry(2)
        .build()
    val nebulaReadEdgeConfig: ReadNebulaConfig = ReadNebulaConfig
      .builder()
      .withSpace(space)
      .withLabel(edgeType)
      .withNoColumn(false)
      .withReturnCols(props)
      .withLimit(limit)
      .withPartitionNum(partition)
      .build()
    val edge = spark.read.nebula(config, nebulaReadEdgeConfig).loadEdgesToDF()
    edge.printSchema()
    edge.show(20)
    println("edge count: " + edge.count())
    edge
  }

  /**
    *
    * we will use edgeAggFields and tagAggField to make join operation.
    * such as:
    * edge.join(vertex).where(edgeAggFields(0)==tagAggField)
    * edge.join(vertex).where(edgeAggFields(1)==tagAggField)
    */
  def aggregateData(edgeDF: DataFrame,
                    tagDF: DataFrame,
                    edgeProps: List[String],
                    tagTypeField: String,
                    edgeAggFields: List[String],
                    tagAggField: String,
                    reduceField: String): DataFrame = {
    val edgeProp1 = edgeAggFields.head
    val edgeProp2 = edgeAggFields.tail.head
    val tagProp   = tagAggField

    val props: ListBuffer[String] = new ListBuffer[String]()
    props.append(edgeProps: _*)
    props.append("sourceType")

    val data = edgeDF
    // join source vertex
      .join(tagDF)
      .where(col(edgeProp1) === col(tagProp))
      .select(tagTypeField, edgeProps: _*)
      .withColumnRenamed(tagTypeField, "sourceType")
      // join target vertex
      .join(tagDF)
      .where(col(edgeProp2) === col(tagProp))
      .select(tagTypeField, props: _*)
      .withColumnRenamed(tagTypeField, "targetType")

    val result = data.groupBy("sourceType", "targetType").agg(sum(reduceField).alias("count"))
    //schema: sourceType, targetType, count
    result
  }

  def writeResult(data: DataFrame,
                  targetGraphAddr: String,
                  targetMetaAddr: String,
                  space: String,
                  edge: String,
                  targetEdgePropField: String,
                  batch: Int,
                  user: String,
                  passwd: String,
                  timeout: Int): Unit = {
    data.show(20)
    val config =
      NebulaConnectionConfig
        .builder()
        .withMetaAddress(targetMetaAddr)
        .withGraphAddress(targetGraphAddr)
        .withConenctionRetry(2)
        .withTimeout(timeout)
        .build()
    val nebulaWriteEdgeConfig: WriteNebulaEdgeConfig = WriteNebulaEdgeConfig
      .builder()
      .withSpace(space)
      .withEdge(edge)
      .withSrcIdField("sourceType")
      .withDstIdField("targetType")
      .withBatch(batch)
      .withUser(user)
      .withPasswd(passwd)
      .build()
    data
      .withColumnRenamed("count", targetEdgePropField)
      .write
      .nebula(config, nebulaWriteEdgeConfig)
      .writeEdges()
  }

}
