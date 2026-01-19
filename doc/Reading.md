# Reading from NebulaGraph

The connector provides three data source options to read data from a NebulaGraph database.

| Option |                        Description                        |          Value          |
|:------:|:---------------------------------------------------------:|:-----------------------:|
|  Node  | use this if you need to read nodes with their properties. | one node type to read.  |
|  Edge  | use this if you need to read edges with their properties. | one edge type to read.  |
|  GQL   |     use this if you need to execute a gql with spark.     | gql statement to query. |

## Examples with encapsulated parameter builder

* read nodes from NebulaGraph

```agsl
    val connectionConfig: NebulaConnectionConfig = NebulaConnectionConfig
      .builder()
      .withGraphAddress("127.0.0.1:9669")
      .withUser("root")
      .withPasswd("nebula")
      .build()
  
    val nebulaNodeReadConfig: ReadNebulaConfig = ReadNebulaConfig
      .builder()
      .withGraphName("movie")
      .withTypeName("Movie")
      .withReturnCols(List("id"))
      .withBatchSize(10)
      .withPartitionNum(1)
      .build()
    val df = spark.read.nebula(connectionConfig, nebulaNodeReadConfig).loadNode()
```

* read edges from NebulaGraph

```agsl
    val connectionConfig: NebulaConnectionConfig = NebulaConnectionConfig
      .builder()
      .withGraphAddress("127.0.0.1:9669")
      .withUser("root")
      .withPasswd("nebula")
      .build()
    val nebulaReadEdgeConfig: ReadNebulaConfig = ReadNebulaConfig
      .builder()
      .withGraphName("movie")
      .withTypeName("Act")
      .withReturnCols(null)
      .withBatchSize(1000)
      .withPartitionNum(1)
      .build()
    val df = spark.read.nebula(connectionConfig, nebulaReadEdgeConfig).loadEdge()
```

* read gql result from NebulaGraph

```agsl
    val connectionConfig: NebulaConnectionConfig = NebulaConnectionConfig
      .builder()
      .withGraphAddress("127.0.0.1:9669")
      .withUser("root")
      .withPasswd("nebula")
      .build()
     val nebulaGqlConfig:GqlNebulaConfig = GqlNebulaConfig
      .builder()
      .withGql("USE movie MATCH(v:Movie) return v.id, v.name")
      .build()
    val df = spark.read.gql(connectionConfig, nebulaGqlConfig).load()
```

## Examples with Options

* read nodes from NebulaGraph

```agsl
     val df = spark
      .read
      .format("com.vesoft.nebula.spark.connector.NebulaDataSource")
      .option("type", "node")
      .option("operate_type", "read")
      .option("graph_address", "127.0.0.1:9669")
      .option("label", "User")
      .option("user", "root")
      .option("passwd", "NebulaGraph01")
      .option("graph_name", "movie")
      .option("batch_size", "10")
      .load()
```

* read edges from NebulaGraph

```agsl
    val df = spark.read
      .format("com.vesoft.nebula.spark.connector.NebulaDataSource")
      .option("type", "edge")
      .option("operate_type", "read")
      .option("graph_address", "127.0.0.1:9669")
      .option("user", "root")
      .option("passwd", "NebulaGraph01")
      .option("graph_name", "movie")
      .option("label", "Watch")
      .option("batch_size", "10")
      .load()
```

* read gql result from NebulaGraph

```agsl
    val df = spark
      .read
      .format("com.vesoft.nebula.spark.connector.NebulaDataSource")
      .option("type", "gql")
      .option("operate_type", "read")
      .option("graph_address", "127.0.0.1:9669")
      .option("user", "root")
      .option("passwd", "NebulaGraph01")
      .option("gql","use movie match(v:Movie) return v.id, v.name")
      .load()
```

## Reader options

|        option name         |                                                                                Description                                                                                |  Default Value  | Required |
|:--------------------------:|:-------------------------------------------------------------------------------------------------------------------------------------------------------------------------:|:---------------:|:--------:|
|            type            |                                                          data type to read, alternative value: node, edge, gql.                                                           |        _        |   true   |
|        operate_type        |                                                              operation type， alternative value: read, write.                                                              |        _        |   true   |
|       graph_address        |                                                   NebulaGraph server address, separate by comma for multiple addresses.                                                   |        _        |   true   |
|            user            |                                                                          NebulaGraph user name.                                                                           |      root       |  false   |
|           passwd           |                                                                      NebulaGraph password for user.                                                                       |        -        |   true   |
|           schema           |                                                                         NebulaGraph schema path.                                                                          | /default_schema |  false   |
|         graph_name         |                                                                          NebulaGraph graph name.                                                                          |        -        |   true   |
|           label            |                                                     NebulaGraph Node type/ edge type name. Used for `type` node/edge.                                                     |        -        |  false   |
|        return_cols         | property names for Node type or Edge type. do not config it for all properties, config "" for no properties, config properties separate by comma for specific properties. |        -        |  false   |
|         batch_size         |                                        Number of records to read for one request between connector and nebula. Used for node/edge.                                        |      1000       |  false   |
|      partition_number      |                                                         Number of partitions to read nebula. Used for node/edge.                                                          |        1        |  false   |
|          timeout           |                                                    timeout for read request between connector and nebula. unit: second                                                    |        3        |  false   |
|    server_ping_timeout     |                                                             timeout for ping NebulaGraph server. unit: second                                                             |        3        |  false   |
|      execution_retry       |                                                               retry times when request NebulaGraph failed.                                                                |        3        |  false   |
|  execution_retry_interval  |                                                              interval need to wait between retries. unit: ms                                                              |        0        |  false   |
|         enable_tls         |                                                                            whether enable tls.                                                                            |      false      |  false   |
| disable_verify_server_cert |                                                         whether to skip the verification for server certificates.                                                         |      false      |  false   |
|           tlsCa            |                                                                       certificate path for server.                                                                        |        -        |  false   |
|          tlsCert           |                                                                       certificate  path for client.                                                                       |        -        |  false   |
|           tlsKey           |                                                                       private key path for client.                                                                        |        -        |  false   |
