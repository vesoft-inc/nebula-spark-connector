/*
 * Copyright (c) 2025 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

package com.vesoft.nebula.spark.common

import org.apache.commons.lang.StringUtils
import org.apache.spark.sql.catalyst.util.CaseInsensitiveMap

import java.util.Properties

class NebulaOptions(@transient val parameters: CaseInsensitiveMap[String]) extends Serializable {

  import NebulaOptions._

  def this(parameters: Map[String, String], operaType: OperaType.Value) =
    this(CaseInsensitiveMap(parameters))

  def this(hostAndPorts: String,
           graphName: String,
           dataType: String,
           label: String,
           parameters: Map[String, String]) = {
    this(
      CaseInsensitiveMap(
        parameters ++ Map(
          NebulaOptions.GRAPH_NAME -> graphName,
          NebulaOptions.TYPE -> dataType,
          NebulaOptions.LABEL -> label
          ))
      )
  }

  val operaType = OperaType.withName(parameters(OPERATE_TYPE))

  /**
   * Return property with all options
   */
  val asProperties: Properties = {
    val properties = new Properties()
    parameters.originalMap.foreach { case (k, v) => properties.setProperty(k, v) }
    properties
  }

  val timeout               : Int                               =
    parameters.getOrElse(TIMEOUT, DEFAULT_CONNECTION_TIMEOUT_SECONDS).toString.toInt
  val pingTimeout           : Int                               =
    parameters.getOrElse(PING_SERVER_TIMEOUT, DEFAULT_PING_TIMEOUT_SECONDS).toString.toInt
  val executionRetry        : Int                               =
    parameters.getOrElse(EXECUTION_RETRY, DEFAULT_EXECUTION_RETRY).toString.toInt
  val executionRetryInterval: Int                               =
    parameters.getOrElse(EXECUTION_RETRY_INTERVAL, DEFAULT_EXECUTION_RETRY_INTERVAL).toString.toInt
  val user                  : String                            = parameters.getOrElse(USER_NAME, DEFAULT_USER_NAME)
  val authOptions           : java.util.HashMap[String, Object] = {
    val authJsonString                                      = parameters.getOrElse(AUTHOPTIONS, "{}")
    val authInfo: java.util.HashMap[String, Object] = AuthOptionsJson.parseObject(authJsonString)
    if (parameters.isDefinedAt(PASSWD)) authInfo.put("password", parameters(PASSWD): Object)
    authInfo
  }
  val rateLimit             : Long                              = parameters.getOrElse(RATE_LIMIT, DEFAULT_RATE_LIMIT).toString.toLong

  require(parameters.isDefinedAt(TYPE), s"Option '$TYPE' is required")
  val dataType: String = parameters(TYPE)
  require(
    DataTypeEnum.validDataType(dataType),
    s"Option '$TYPE' is illegal, it should be '${DataTypeEnum.NODE}' or '${DataTypeEnum.EDGE}' or `${DataTypeEnum.GQL}`")

  /** tls parameters */
  val enableTls              : Boolean = parameters.getOrElse(ENABLE_TLS, false).toString.toBoolean
  val disableVerifyServerCert: Boolean = parameters.getOrElse(DISABLE_VERIFY_SERVER_CERT, false).toString.toBoolean
  var tlsCa                            = if (enableTls) parameters.getOrElse(TLS_CA, null) else null
  val tlsCert                          = if (enableTls) parameters.getOrElse(NebulaOptions.TLS_CERT, null) else null
  val tlsKey                           = if (enableTls) parameters.getOrElse(NebulaOptions.TLS_KEY, null) else null

  /** nebula common parameters */
  require(parameters.isDefinedAt(GRAPH_ADDRESS),
          s"option $GRAPH_ADDRESS is required and can not be blank")
  var graphAddress      = parameters(GRAPH_ADDRESS)
  val schema            = parameters.getOrElse[String](SCHEMA, null)
  var graphName: String = _
  var label    : String = _
  if (!dataType.equalsIgnoreCase(DataTypeEnum.GQL.toString)) {
    require(parameters.isDefinedAt(GRAPH_NAME) && StringUtils.isNotBlank(parameters(GRAPH_NAME)),
            s"Option '$GRAPH_NAME' is required and can not be blank")
    graphName = parameters(GRAPH_NAME)
    require(parameters.isDefinedAt(LABEL) && StringUtils.isNotBlank(parameters(LABEL)),
            s"Option '$LABEL' is required and can not be blank")
    label = parameters(LABEL)
  }

  var batchSize: Int = parameters.getOrElse(BATCH_SIZE, DEFAULT_BATCH_SIZE).toString.toInt

  /** write parameters */

  var srcPkFields        : List[String]    = _
  var dstPkFields        : List[String]    = _
  var srcPksAsProp       : Boolean         = _
  var dstPksAsProp       : Boolean         = _
  var writeMode          : WriteMode.Value = _
  var disableWriteLog    : Boolean         = _
  var errorWhenFailed    : Boolean         = _
  var dateFormat         : String          = _
  var zonedDatetimeFormat: String          = _
  var localDatetimeFormat: String          = _
  var zonedTimeFormat    : String          = _
  var localTimeFormat    : String          = _

  if (operaType == OperaType.WRITE) {
    srcPkFields = parameters.getOrElse(SRC_PK_FIELD, "").split("&&").toList
    dstPkFields = parameters.getOrElse(DST_PK_FIELD, "").split("&&").toList
    srcPksAsProp = parameters.getOrElse(SRC_PK_AS_PROP, false).toString.toBoolean
    dstPksAsProp = parameters.getOrElse(DST_PK_AS_PROP, false).toString.toBoolean
    writeMode =
      WriteMode.withName(parameters.getOrElse(WRITE_MODE, DEFAULT_WRITE_MODE).toString.toLowerCase)
    disableWriteLog = parameters.getOrElse(DISABLE_WRITE_LOG, false).toString.toBoolean
    errorWhenFailed = parameters.getOrElse(ERROR_WHEN_FAILED, false).toString.toBoolean
    dateFormat = parameters.getOrElse[String](DATE_FORMAT, null)
    zonedDatetimeFormat = parameters.getOrElse[String](ZONED_DATETIME_FORMAT, null)
    localDatetimeFormat = parameters.getOrElse[String](LOCAL_DATETIME_FORMAT, null)
    zonedTimeFormat = parameters.getOrElse[String](ZONED_TIME_FORMAT, null)
    localTimeFormat = parameters.getOrElse[String](LOCAL_TIME_FORMAT, null)
  }

  /** read parameters */
  var partitionNums: Int = parameters.getOrElse(PARTITION_NUMBER, "1").toInt
  val returnCols         = parameters.getOrElse(RETURN_COLS, null)

  def getReturnCols: List[String] = {
    if (returnCols == null) {
      null
    } else if (returnCols.isEmpty) {
      List()
    } else {
      returnCols.split(",").toList
    }
  }

  /** gql parameters */
  var gql: String = parameters.getOrElse(GQL, null)
}

private[common] object AuthOptionsJson {
  def stringify(values: collection.Map[String, Any]): String = {
    values.iterator
      .map { case (key, value) => quote(key) + ":" + toJson(value) }
      .mkString("{", ",", "}")
  }

  def parseObject(authJsonString: String): java.util.HashMap[String, Object] = {
    val result = new java.util.HashMap[String, Object]()
    val json   = Option(authJsonString).map(_.trim).getOrElse("")

    if (json.isEmpty || json == "{}") {
      return result
    }

    require(json.startsWith("{") && json.endsWith("}"),
            s"Option '${NebulaOptions.AUTHOPTIONS}' must be a JSON object string.")

    val content = json.substring(1, json.length - 1).trim
    if (content.isEmpty) {
      return result
    }

    splitTopLevel(content, ',').foreach { item =>
      val (rawKey, rawValue) = splitKeyValue(item)
      result.put(parseJsonString(rawKey.trim), parseJsonValue(rawValue.trim))
    }

    result
  }

  private def toJson(value: Any): String = value match {
    case null => "null"
    case s: String => quote(s)
    case c: Char => quote(c.toString)
    case b: Boolean => b.toString
    case n: Byte => n.toString
    case n: Short => n.toString
    case n: Int => n.toString
    case n: Long => n.toString
    case n: Float => n.toString
    case n: Double => n.toString
    case m: collection.Map[_, _] =>
      m.iterator
        .map { case (k, v) => quote(String.valueOf(k)) + ":" + toJson(v) }
        .mkString("{", ",", "}")
    case m: java.util.Map[_, _] =>
      import scala.collection.JavaConverters.mapAsScalaMapConverter

      m.asScala.iterator
        .map { case (k, v) => quote(String.valueOf(k)) + ":" + toJson(v) }
        .mkString("{", ",", "}")
    case it: Iterable[_] =>
      it.iterator.map(toJson).mkString("[", ",", "]")
    case it: java.lang.Iterable[_] =>
      import scala.collection.JavaConverters.iterableAsScalaIterableConverter

      it.asScala.iterator.map(toJson).mkString("[", ",", "]")
    case other => quote(String.valueOf(other))
  }

  private def quote(s: String): String = "\"" + escape(s) + "\""

  private def escape(s: String): String = {
    val sb = new StringBuilder(s.length + 16)
    s.foreach {
      case '"'  => sb.append("\\\"")
      case '\\' => sb.append("\\\\")
      case '\b' => sb.append("\\b")
      case '\f' => sb.append("\\f")
      case '\n' => sb.append("\\n")
      case '\r' => sb.append("\\r")
      case '\t' => sb.append("\\t")
      case ch if ch < ' ' => sb.append(f"\\u${ch.toInt}%04x")
      case ch => sb.append(ch)
    }
    sb.toString()
  }

  private def splitKeyValue(entry: String): (String, String) = {
    val index = findTopLevelChar(entry, ':')
    require(index >= 0, s"Invalid auth option entry: $entry")
    (entry.substring(0, index), entry.substring(index + 1))
  }

  private def splitTopLevel(input: String, separator: Char): List[String] = {
    val result  = scala.collection.mutable.ListBuffer[String]()
    val current = new StringBuilder

    var inQuotes   = false
    var escaped    = false
    var braceDepth = 0
    var arrayDepth = 0

    input.foreach { ch =>
      if (escaped) {
        current.append(ch)
        escaped = false
      } else {
        ch match {
          case '\\' =>
            current.append(ch)
            escaped = true
          case '"' =>
            current.append(ch)
            inQuotes = !inQuotes
          case '{' if !inQuotes =>
            current.append(ch)
            braceDepth += 1
          case '}' if !inQuotes =>
            current.append(ch)
            braceDepth -= 1
          case '[' if !inQuotes =>
            current.append(ch)
            arrayDepth += 1
          case ']' if !inQuotes =>
            current.append(ch)
            arrayDepth -= 1
          case c if c == separator && !inQuotes && braceDepth == 0 && arrayDepth == 0 =>
            result += current.toString().trim
            current.clear()
          case _ =>
            current.append(ch)
        }
      }
    }

    if (current.nonEmpty) {
      result += current.toString().trim
    }

    result.toList.filter(_.nonEmpty)
  }

  private def findTopLevelChar(input: String, target: Char): Int = {
    var inQuotes   = false
    var escaped    = false
    var braceDepth = 0
    var arrayDepth = 0
    var index      = 0

    while (index < input.length) {
      val ch = input.charAt(index)
      if (escaped) {
        escaped = false
      } else {
        ch match {
          case '\\' =>
            escaped = true
          case '"' =>
            inQuotes = !inQuotes
          case '{' if !inQuotes =>
            braceDepth += 1
          case '}' if !inQuotes =>
            braceDepth -= 1
          case '[' if !inQuotes =>
            arrayDepth += 1
          case ']' if !inQuotes =>
            arrayDepth -= 1
          case c if c == target && !inQuotes && braceDepth == 0 && arrayDepth == 0 =>
            return index
          case _ =>
        }
      }
      index += 1
    }

    -1
  }

  private def parseJsonValue(token: String): Object = {
    if (token == "null") {
      null
    } else if (token == "true" || token == "false") {
      java.lang.Boolean.valueOf(token)
    } else if (token.startsWith("\"") && token.endsWith("\"")) {
      parseJsonString(token)
    } else {
      parseJsonNumberOrString(token)
    }
  }

  private def parseJsonNumberOrString(token: String): Object = {
    try {
      if (token.contains(".") || token.contains("e") || token.contains("E")) {
        java.lang.Double.valueOf(token)
      } else {
        val longValue = java.lang.Long.valueOf(token)
        if (longValue >= Int.MinValue && longValue <= Int.MaxValue) {
          java.lang.Integer.valueOf(longValue.toInt)
        } else {
          longValue
        }
      }
    } catch {
      case _: NumberFormatException => token
    }
  }

  private def parseJsonString(token: String): String = {
    require(token.length >= 2 && token.head == '"' && token.last == '"',
            s"Invalid JSON string: $token")

    val sb = new StringBuilder
    var i  = 1

    while (i < token.length - 1) {
      val ch = token.charAt(i)
      if (ch != '\\') {
        sb.append(ch)
        i += 1
      } else {
        require(i + 1 < token.length - 1, s"Invalid escape in JSON string: $token")
        token.charAt(i + 1) match {
          case '"'  => sb.append('"'); i += 2
          case '\\' => sb.append('\\'); i += 2
          case '/'  => sb.append('/'); i += 2
          case 'b'  => sb.append('\b'); i += 2
          case 'f'  => sb.append('\f'); i += 2
          case 'n'  => sb.append('\n'); i += 2
          case 'r'  => sb.append('\r'); i += 2
          case 't'  => sb.append('\t'); i += 2
          case 'u' =>
            require(i + 5 < token.length, s"Invalid unicode escape in JSON string: $token")
            val hex = token.substring(i + 2, i + 6)
            sb.append(Integer.parseInt(hex, 16).toChar)
            i += 6
          case other =>
            throw new IllegalArgumentException(s"Unsupported escape sequence: \\$other")
        }
      }
    }

    sb.toString()
  }
}

object NebulaOptions {

  /** nebula common config */
  val GRAPH_NAME           : String = "graph_name"
  val SCHEMA               : String = "schema"
  val DATE_FORMAT          : String = "date_format"
  val ZONED_DATETIME_FORMAT: String = "zoned_datetime_format"
  val LOCAL_DATETIME_FORMAT: String = "local_datetime_format"
  val ZONED_TIME_FORMAT    : String = "zoned_time_format"
  val LOCAL_TIME_FORMAT    : String = "local_time_format"
  val GRAPH_ADDRESS        : String = "graph_address"
  val TYPE                 : String = "type"
  val LABEL                : String = "label"
  val BATCH_SIZE           : String = "batch_size"

  /** connection config */
  val TIMEOUT                   : String = "timeout"
  val EXECUTION_RETRY           : String = "execution_retry"
  val EXECUTION_RETRY_INTERVAL  : String = "execution_retry_interval"
  val USER_NAME                 : String = "user"
  val PASSWD                    : String = "passwd"
  val AUTHOPTIONS               : String = "auth_options"
  val ENABLE_TLS                : String = "enable_tls"
  val DISABLE_VERIFY_SERVER_CERT: String = "disable_verify_server_cert"
  val TLS_CA                    : String = "tlsCa"
  val TLS_CERT                  : String = "tlsCert"
  val TLS_KEY                   : String = "tlsKey"
  val PING_SERVER_TIMEOUT       : String = "server_ping_timeout"

  val OPERATE_TYPE: String = "operate_type"

  /** gql config */
  val GQL: String = "gql"

  /** write config */
  val RATE_LIMIT       : String = "rate_limit"
  val SRC_PK_FIELD              = "src_pk_field"
  val DST_PK_FIELD              = "dst_pk_field"
  val SRC_PK_AS_PROP   : String = "src_pk_as_prop"
  val DST_PK_AS_PROP   : String = "dst_pk_as_prop"
  val WRITE_MODE       : String = "write_mode"
  val DISABLE_WRITE_LOG: String = "disable_write_log"
  val ERROR_WHEN_FAILED: String = "error_when_failed"

  /** read config */
  val PARTITION_NUMBER: String = "partition_number"
  val RETURN_COLS     : String = "return_cols"

  val DEFAULT_TIMEOUT_SECONDS           : Int    = 10
  val DEFAULT_CONNECTION_TIMEOUT_SECONDS: Int    = 3
  val DEFAULT_PING_TIMEOUT_SECONDS      : Int    = 3
  val DEFAULT_CONNECTION_RETRY          : Int    = 3
  val DEFAULT_EXECUTION_RETRY           : Int    = 3
  val DEFAULT_EXECUTION_RETRY_INTERVAL  : Int    = 0
  val DEFAULT_USER_NAME                 : String = "root"
  val DEFAULT_PASSWD                    : String = "nebula"

  val DEFAULT_RATE_LIMIT: Long = 1024L
  val DEFAULT_BATCH_SIZE: Int  = 1000

  val DEFAULT_WRITE_MODE = WriteMode.INSERT

  val EMPTY_STRING: String = ""
}
