/* Copyright (c) 2023 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

package com.vesoft.nebula.examples.connector

import com.vesoft.nebula.client.meta.MetaClient
import com.vesoft.nebula.connector.{NebulaConnectionConfig, ReadNebulaConfig}
import com.vesoft.nebula.connector.connector.NebulaDataFrameReader
import org.apache.spark.SparkConf
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.commons.cli.{
  CommandLine,
  CommandLineParser,
  HelpFormatter,
  Option,
  Options,
  ParseException,
  PosixParser
}
import org.slf4j.LoggerFactory
import shaded.parquet.org.apache.thrift.protocol.TCompactProtocol

import scala.collection.JavaConverters.asScalaBufferConverter
import scala.collection.mutable.ListBuffer

object Nebula2File {
  private val LOG = LoggerFactory.getLogger(this.getClass)
  var countData   = false
  def main(args: Array[String]): Unit = {

    // config the parameters
    val sourceMetaOption =
      new Option("sourceMeta", "sourceMetaAddress", true, "source nebulagraph metad address")
    sourceMetaOption.setRequired(true)
    val sourceSpaceOption =
      new Option("sourceSpace", "sourceSpace", true, "source nebulagraph space name")
    sourceSpaceOption.setRequired(true)
    val limitOption =
      new Option("limit",
                 "limit",
                 true,
                 "records for one reading request for reading, default 1000")
    val noFieldsOption = new Option("noFields",
                                    "noFields",
                                    true,
                                    "no property field for reading, true or false, default false")

    val targetFileSystemOption =
      new Option("targetFileSystem",
                 "targetFileSystem",
                 true,
                 "target file system to save Nebula data, support hdfs,oss,s3, default hdfs")

    val targetFileSysAccessKeyOption =
      new Option("accessKey", "accessKey", true, "access key for oss or s3")
    val targetFileSysSecretKeyOption =
      new Option("secretKey", "secretKey", true, "secret key for oss or s3")
    val targetFileSysEndpointOption =
      new Option("endpoint", "endpoint", true, "endpoint for oss or s3")

    val targetFileFormatOption =
      new Option("targetFileFormat",
                 "targetFileFormat",
                 true,
                 "target file format to save Nebula data, support csv, parquet, json, default csv")

    val csvHeaderOption =
      new Option("header", "header", true, "if write header for csv format, default true")
    val csvDelimiterOption =
      new Option("delimiter", "delimiter", true, "csv delimiter, default is ','")
    val targetFilePathOption =
      new Option("targetFilePath", "targetFilePath", true, "target file path to save Nebula data")
    targetFilePathOption.setRequired(true)

    // filter out some tags /edges
    val excludeTagsOption =
      new Option("excludeTags", "excludeTags", true, "filter out these tags, separate with `,`")
    val excludeEdgesOption =
      new Option("excludeEdges", "excludeEdges", true, "filter out these edges, separate with `,`")
    val includeTagOption =
      new Option("includeTag", "includeTag", true, "only migrate the specific tag")

    // if count each tag/edge's data when export
    val countOption = new Option("count",
                                 "count",
                                 true,
                                 "if count each tag/edge's count when export, default is false")

    val options = new Options
    options.addOption(sourceMetaOption)
    options.addOption(sourceSpaceOption)
    options.addOption(limitOption)
    options.addOption(noFieldsOption)
    options.addOption(targetFileSystemOption)
    options.addOption(targetFileFormatOption)
    options.addOption(csvHeaderOption)
    options.addOption(csvDelimiterOption)
    options.addOption(targetFilePathOption)
    options.addOption(excludeTagsOption)
    options.addOption(excludeEdgesOption)
    options.addOption(includeTagOption)
    options.addOption(targetFileSysAccessKeyOption)
    options.addOption(targetFileSysSecretKeyOption)
    options.addOption(targetFileSysEndpointOption)
    options.addOption(countOption)

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

    val sourceMetaAddr: String = cli.getOptionValue("sourceMeta")
    val sourceSpace: String    = cli.getOptionValue("sourceSpace")
    val limit: Int             = if (cli.hasOption("limit")) cli.getOptionValue("limit").toInt else 1000
    val noFields: Boolean =
      if (cli.hasOption("noFields")) cli.getOptionValue("noFields").toBoolean else false
    val targetFileSystem: String =
      if (cli.hasOption("targetFileSystem")) cli.getOptionValue("targetFileSystem") else "hdfs"
    val targetFileFormat: String =
      if (cli.hasOption("targetFileFormat")) cli.getOptionValue("targetFileFormat") else "csv"
    val targetFilePath: String = cli.getOptionValue("targetFilePath")
    val excludeTags: List[String] =
      if (cli.hasOption("excludeTags")) cli.getOptionValue("excludeTags").split(",").toList
      else List()
    val excludeEdges: List[String] =
      if (cli.hasOption("excludeEdges")) cli.getOptionValue("excludeEdges").split(",").toList
      else List()
    val includeTag: String =
      if (cli.hasOption("includeTag")) cli.getOptionValue("includeTag") else null

    val fileSys    = FileSystemCategory.withName(targetFileSystem.trim.toUpperCase)
    val fileFormat = FileFormatCategory.withName(targetFileFormat.trim.toUpperCase)

    val header    = if (cli.hasOption("csvHeader")) cli.getOptionValue("csvHeader").toBoolean else true
    val delimiter = if (cli.hasOption("csvDelimiter")) cli.getOptionValue("csvDelimiter") else ","

    val accessKey = if (cli.hasOption("accessKey")) cli.getOptionValue("accessKey") else null
    val secretKey = if (cli.hasOption("secretKey")) cli.getOptionValue("secretKey") else null
    val endpoint  = if (cli.hasOption("endpoint")) cli.getOptionValue("endpoint") else null
    countData = if (cli.hasOption("count")) cli.getOptionValue("count").toBoolean else false

    LOG.info(s"""options:
                |source meta address: $sourceMetaAddr
                |source space name: $sourceSpace
                |read limit for one request :$limit
                |read property with noFields: $noFields
                |read tags without some tags, excludeTags: $excludeTags
                |read edges without some edges, excludeEdges: $excludeEdges
                |read tag to do test, includeTag:$includeTag
                |target file system: $targetFileSystem
                |target file format: $targetFileFormat
                |target file path:$targetFilePath
                |target access key for oss or s3: $accessKey
                |target secret key for oss or s3:$secretKey
                |target endpoint for oss or s3: $endpoint
                |target csv header: $header
                |target csv delimiter: $delimiter
                |if count data while export: $countData
                |""".stripMargin)

    // common config
    val sourceConnectConfig =
      NebulaConnectionConfig
        .builder()
        .withMetaAddress(sourceMetaAddr)
        .withConnectionRetry(2)
        .withTimeout(10 * 1000)
        .build()

    // get all tags and edges for space
    val metaHostAndPort = sourceMetaAddr.split(":")
    var (tags, edges, partitions) =
      getTagsAndEdges(metaHostAndPort(0), metaHostAndPort(1).toInt, sourceSpace)

    // get spark with specific file system, hdfs or s3 or oss
    val spark = getSpark(fileSys, accessKey, secretKey, endpoint)
    spark.conf.set("mapreduce.fileoutputcommitter.marksuccessfuljobs", "false")

    val startTime = System.currentTimeMillis()

    // test with one specific tag to export
    if (includeTag != null) {
      LOG.info(s" >>>>>> source space tag: ${includeTag}")
      val start = System.currentTimeMillis()
      exportTag(spark,
                sourceConnectConfig,
                sourceSpace,
                limit,
                partitions,
                includeTag,
                noFields,
                fileFormat,
                targetFilePath,
                header,
                delimiter)
      LOG.info(
        s" >>>>>> finished export tag $includeTag, cost: ${System.currentTimeMillis() - start}ms")
      spark.stop()
      System.exit(0)
    }

    // get tags need to be export
    val exportTags = new ListBuffer[String]
    LOG.info(s" >>>>>> source space tags: ${tags}")
    LOG.info(s" >>>>>> exclude tags: ${excludeTags}")
    for (i <- tags.indices) {
      if (!excludeTags.contains(tags(i))) {
        exportTags.append(tags(i))
      }
    }
    LOG.info(s" >>>>>> tags need to export: $exportTags")

    // get edges need to be export
    val exportEdges = new ListBuffer[String]
    LOG.info(s" >>>>>> source space edges: ${edges}")
    LOG.info(s" >>>>>> exclude edges: ${excludeEdges}")
    for (i <- edges.indices) {
      if (!excludeEdges.contains(edges(i))) {
        exportEdges.append(edges(i))
      }
    }
    LOG.info(s" >>>>>> edges need to export: ${exportEdges}")

    // start to export
    exportTags.par.foreach(tag => {
      val start = System.currentTimeMillis()
      exportTag(spark,
                sourceConnectConfig,
                sourceSpace,
                limit,
                partitions,
                tag,
                noFields,
                fileFormat,
                targetFilePath,
                header,
                delimiter)
      LOG.info(s" >>>>>> finished export tag: $tag, cost: ${System.currentTimeMillis() - start}ms")
    })

    exportEdges.par.foreach(edge => {
      val start = System.currentTimeMillis()
      exportEdge(spark,
                 sourceConnectConfig,
                 sourceSpace,
                 limit,
                 partitions,
                 edge,
                 noFields,
                 fileFormat,
                 targetFilePath,
                 header,
                 delimiter)
      LOG.info(s" >>>>>> finished export edge: $edge, cost:${System.currentTimeMillis() - start}ms")
    })
    LOG.info(s" >>>>>> export job finished, cost: ${System.currentTimeMillis() - startTime}ms")
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
    (tags.toList.distinct, edges.toList.distinct, partitions)
  }

  def exportTag(spark: SparkSession,
                sourceConfig: NebulaConnectionConfig,
                sourceSpace: String,
                limit: Int,
                readPartition: Int,
                tag: String,
                noFields: Boolean,
                fileFormat: FileFormatCategory.Value,
                filePath: String,
                header: Boolean,
                delimiter: String): Unit = {
    LOG.info(s" >>>>>> start to sync tag ${tag}")
    spark.sparkContext.setJobGroup(s"$tag", s"export tag:$tag")
    val nebulaReadVertexConfig: ReadNebulaConfig = ReadNebulaConfig
      .builder()
      .withSpace(sourceSpace)
      .withLabel(tag)
      .withNoColumn(noFields)
      .withReturnCols(List())
      .withLimit(limit)
      .withPartitionNum(readPartition)
      .build()
    val vertex = spark.read.nebula(sourceConfig, nebulaReadVertexConfig).loadVerticesToDF()

    if (countData) {
      LOG.info(s" >>>>>> tag $tag count: ${vertex.count()}")
    }
    val path = s"$filePath/$tag"
    fileFormat match {
      case FileFormatCategory.CSV =>
        vertex.write
          .mode(SaveMode.Overwrite)
          .mode(SaveMode.Overwrite)
          .option("header", header)
          .option("delimiter", delimiter)
          .csv(path)
      case FileFormatCategory.JSON    => vertex.write.mode(SaveMode.Overwrite).json(path)
      case FileFormatCategory.PARQUET => vertex.write.mode(SaveMode.Overwrite).parquet(path)
    }

  }

  def exportEdge(spark: SparkSession,
                 sourceConfig: NebulaConnectionConfig,
                 sourceSpace: String,
                 limit: Int,
                 readPartition: Int,
                 edge: String,
                 noFields: Boolean,
                 fileFormat: FileFormatCategory.Value,
                 filePath: String,
                 header: Boolean,
                 delimiter: String): Unit = {
    LOG.info(s" >>>>>> start to sync edge ${edge}")
    spark.sparkContext.setJobGroup(s"$edge", s"export edge:$edge")
    val nebulaReadEdgeConfig: ReadNebulaConfig = ReadNebulaConfig
      .builder()
      .withSpace(sourceSpace)
      .withLabel(edge)
      .withNoColumn(noFields)
      .withReturnCols(List())
      .withLimit(limit)
      .withPartitionNum(readPartition)
      .build()
    val edgeDf = spark.read.nebula(sourceConfig, nebulaReadEdgeConfig).loadEdgesToDF()

    if (countData) {
      LOG.info(s" >>>>>> edge $edge count: ${edgeDf.count()}")
    }

    val path = s"$filePath/$edge"
    fileFormat match {
      case FileFormatCategory.CSV =>
        edgeDf.write
          .mode(SaveMode.Overwrite)
          .option("header", header)
          .option("delimiter", delimiter)
          .csv(path)
      case FileFormatCategory.JSON    => edgeDf.write.mode(SaveMode.Overwrite).json(path)
      case FileFormatCategory.PARQUET => edgeDf.write.mode(SaveMode.Overwrite).parquet(path)
    }
  }

  def getSpark(fileSys: FileSystemCategory.Value,
               accessKey: String,
               secretKey: String,
               endpoint: String): SparkSession = {
    val sparkConf = new SparkConf
    sparkConf
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .registerKryoClasses(Array[Class[_]](classOf[TCompactProtocol]))
    val spark = SparkSession
      .builder()
      .config(sparkConf)
      .getOrCreate()

    if (fileSys == FileSystemCategory.S3) {
      spark.sparkContext.hadoopConfiguration.set("fs.s3a.access.key", accessKey)
      spark.sparkContext.hadoopConfiguration.set("fs.s3a.secret.key", secretKey)
      spark.sparkContext.hadoopConfiguration.set("fs.s3a.endpoint", endpoint)
    }
    if (fileSys == FileSystemCategory.OSS) {
      spark.sparkContext.hadoopConfiguration.set("fs.oss.accessKey", accessKey)
      spark.sparkContext.hadoopConfiguration.set("fs.oss.secretKey", secretKey)
      spark.sparkContext.hadoopConfiguration.set("fs.oss.endPoint", endpoint)
      spark.sparkContext.hadoopConfiguration
        .set("fs.oss.impl", "org.apache.hadoop.fs.aliyun.oss.AliyunOSSFileSystem")
    }
    spark
  }

  object FileSystemCategory extends Enumeration {
    type Type = Value
    val HDFS = Value("HDFS")
    val OSS  = Value("OSS")
    val S3   = Value("S3")
  }

  object FileFormatCategory extends Enumeration {
    type Type = Value
    val CSV     = Value("CSV")
    val PARQUET = Value("PARQUET")
    val JSON    = Value("JSON")
  }
}
