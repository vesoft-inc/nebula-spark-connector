# Writing Dataframe to NebulaGraph

The connector provides two sink data options to write data into a NebulaGraph database.

| Option |                                        Description                                        |          Value          |
|:------:|:-----------------------------------------------------------------------------------------:|:-----------------------:|
|  Node  | use this if you need to write DataFrame with their properties to NebulaGraph's node type. | one node type to write. |
|  Edge  | use this if you need to write DataFrame with their properties to NebulaGraph's edge type. | one edge type to write. |

## Examples with encapsulated parameter builder

* write DataFrame to NebulaGraph Node type

```agsl
    val df = spark.read.option("header", true).csv("movie.csv")
    val connectionConfig: NebulaConnectionConfig = NebulaConnectionConfig
      .builder()
      .withGraphAddress("127.0.0.1:9669")
      .withUser("root")
      .withPasswd("nebula")
      .build()
  
    val nebulaWriteNodeConfig: WriteNebulaNodeConfig = WriteNebulaNodeConfig
      .builder()
      .withGraphName("movie")
      //.withSchema("/default_schema")
      //.withZonedDatetimeFormat("%Y-%m-%dT%H:%M:%S %z")
      //.withLocalDatetimeFormat("%Y-%m-%dT%H:%M:%S")
      //.withZonedTimeFormat("%H:%M:%S %z")
      //.withLocalTimeFormat("%H:%M:%S")
      .withNodeType("Movie")
      .withWriteMode(WriteMode.INSERTIGNORE)
      .withBatchSize(10)
      .build()
    df.write.nebula(getNebulaConnectionConfig, nebulaWriteNodeConfig).writeNodes()
```

* write DataFrame to NebulaGraph Edge type

```agsl
    val df = spark.read.option("header", true).csv("act.csv")
    val connectionConfig: NebulaConnectionConfig = NebulaConnectionConfig
      .builder()
      .withGraphAddress("127.0.0.1:9669")
      .withUser("root")
      .withPasswd("nebula")
      .build()
    val nebulaWriteEdgeConfig: WriteNebulaEdgeConfig = WriteNebulaEdgeConfig
      .builder()
      .withGraphName("movie")
      //.withSchema("/default_schema")
      //.withZonedDatetimeFormat("%Y-%m-%dT%H:%M:%S %z")
      //.withLocalDatetimeFormat("%Y-%m-%dT%H:%M:%S")
      //.withZonedTimeFormat("%H:%M:%S %z")
      //.withLocalTimeFormat("%H:%M:%S")
      .withEdge("Act")
      .withSrcPkFields(List("actor_id"))
      .withDstPkFields(List("movie_id"))
      .withSrcPksAsProperty(false)
      .withDstPksAsProperty(false)
      .withWriteMode(WriteMode.INSERTIGNORE)
      .withBatchSize(10)
      .build()
    df.write.nebula(getNebulaConnectionConfig, nebulaWriteEdgeConfig).writeEdges()
```

## Examples with Options

* write DataFrame to NebulaGraph Node type

```agsl
      val df = spark.read.option("header", true).csv("movie.csv")
      df
        .write
        .format("com.vesoft.nebula.spark.connector.NebulaDataSource")
        .mode("Overwrite")
        .option("type", "node")
        .option("operate_type", "write")
        .option("graph_address", "127.0.0.1:9669")
        .option("user","root")
        .option("passwd", "NebulaGraph01")
        .option("graph_name", "movie")
        .option("label", "movie")
        .option("write_mode", "insert_ignore")
        .option("batch_size","10")
        .save()
```

* write DataFrame to NebulaGraph Edge type

```agsl
      val df = spark.read.option("header", true).csv("act.csv")
      df
        .write
        .format("com.vesoft.nebula.spark.connector.NebulaDataSource")
        .mode("Overwrite")
        .option("type","edge")
        .option("operate_type", "write")
         .option("graph_address", "127.0.0.1:9669")
        .option("user", "root")
        .option("passwd", "NebulaGraph01")
        .option("graph_name", "movie")
         .option("label", "Act")
         .option("src_pk_field", "src_col1")
         .option("dst_pk_field", "dst_col1")
         .option("write_mode", "insert_ignore")
         .option("batch_size", "10")
         .save()
```

## writer options

|        option name         |                                                                  Description                                                                  |  Default Value  | Required |
|:--------------------------:|:---------------------------------------------------------------------------------------------------------------------------------------------:|:---------------:|:--------:|
|            type            |                                            data type to read, alternative value: node, edge, gql.                                             |        _        |   true   |
|        operate_type        |                                                operation type， alternative value: read, write.                                                |        _        |   true   |
|       graph_address        |                                     NebulaGraph server address, separate by comma for multiple addresses.                                     |        _        |   true   |
|            user            |                                                            NebulaGraph user name.                                                             |      root       |  false   |
|           passwd           |                                                        NebulaGraph password for user.                                                         |        -        |   true   |
|           schema           |                                                           NebulaGraph schema path.                                                            | /default_schema |  false   |
|         graph_name         |                                                            NebulaGraph graph name.                                                            |        -        |   true   |
|        date_format         |                               date format for date type property, if not config this, use NebulaGraph's format.                               |        -        |  false   |
|   zoned_datetime_format    |                     zoned datetime format for zoned_datetime type property, if not configured, use NebulaGraph's format.                      |        -        |  false   |
|   local_datetime_format    |                     local datetime format for zoned_datetime type property, if not configured, use NebulaGraph's format.                      |        -        |  false   |
|     zoned_time_format      |                       zoned time format for zoned_datetime type property, if not configured, use NebulaGraph's format.                        |        -        |  false   |
|     local_time_format      |                       local time format for zoned_datetime type property, if not configured, use NebulaGraph's format.                        |        -        |  false   |
|           label            |                                       NebulaGraph Node type/ edge type name. Used for `type` node/edge.                                       |        -        |  false   |
|        src_pk_field        |                         the fields in DataFrame to act as edge source node type's primary key. Used for `type` edge.                          |        -        |  false   |
|        dst_pk_field        |                         the fields in DataFrame to act as edge target node type's primary key. Used for `type` edge.                          |        -        |  false   |
|       src_pk_as_prop       |   Whether the fields which act as edge source node type's primary key also act as edge's property, separate by comma. Used for `type` edge.   |        -        |  false   |
|       dst_pk_as_prop       |   Whether the fields which act as edge target node type's primary key also act as edge's property, separate by comma. Used for `type` edge.   |        -        |  false   |
|         write_mode         |   Write mode when write node/edge, alternative value: insert, insert_replace, insert_ignore, insert_update, update, delete, detach_delete.    |     insert      |  false   |
|         batch_size         |                          Number of records to read for one request between connector and nebula. Used for node/edge.                          |      1000       |  false   |
|     disable_write_log      | Whether disable the log for each request succeed response, which includes batchSize for one write request, latency of this request and so on. |      false      |  false   |
|          timeout           |                                      timeout for read request between connector and nebula. unit: second                                      |        3        |  false   |
|      execution_retry       |                                                 retry times when request NebulaGraph failed.                                                  |        3        |  false   |
|  execution_retry_interval  |                                                interval need to wait between retries. unit: ms                                                |        0        |  false   |
|         enable_tls         |                                                              whether enable tls.                                                              |      false      |  false   |
| disable_verify_server_cert |                                           whether to skip the verification for server certificates.                                           |      false      |  false   |
|           tlsCa            |                                                         certificate path for server.                                                          |        -        |  false   |
|          tlsCert           |                                                         certificate  path for client.                                                         |        -        |  false   |
|           tlsKey           |                                                         private key path for client.                                                          |        -        |  false   |
