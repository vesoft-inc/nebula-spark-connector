## 数据聚合工具

refer https://confluence.nebula-graph.io/pages/viewpage.action?pageId=72600477

如何打包
```
cd delivery-utils/POC/cfets/nebula-aggregate-tool
sh build.sh 
```
build 结束会在 delivery-utils/POC/cfets/nebula-aggregate-tool 目录下生成 nebula-poc-aggregate.jar

使用参数：
```
usage: >>>> options
 -b,--batch <arg>                                   batch size to write aggregate result into edge
 -e,--edge <arg>                                    NebulaGraph edge type for scanning data
 -efields,--edgeAggregateFields <arg>               aggregate fields in edge, used by join condition with tag data, split by comma
 -ep,--edgeprops <arg>                              edge props to scan out, split by comma
 -l,--limit <arg>                                   records amount for one reading request for reading
 -m,--metaAddress <arg>                             NebulaGraph metad address for scanning data
 -p,--partition <arg>                               partition for spark while scanning data, upper limit is nebula space partition_num
 -passwd,--password <arg>                           target NebulaGraph password, used for writing result
 -reduceField,--reduceField <arg>                   reduce field for edge, such as 金额
 -space,--space <arg>                               NebulaGraph space name for scanning data
 -t,--tag <arg>                                     NebulaGraph tag name for scanning data
 -tagTypeField,--tagTypeField <arg>                 tag prop name defined tag's type
 -targetEdge,--targetEdge <arg>                     target edge type used for writing result
 -targetEdgePropField,--targetEdgePropField <arg>   target edge prop field to save the count of amount
 -targetGraphAddr,--targetGraphAddr <arg>           target graph address used for writing result, split by comma
 -targetMetaAddr,--targetMetaAddr <arg>             target meta address used for writing result
 -targetSpace,--targetSpace <arg>                   target space used for writing aggregate result
 -tfield,--tagAggregateField <arg>                  aggregate field in vertex, "_vertexId" for vid, used by join condition with edge data
 -timeout,--timeout <arg>                           timeout for request, used for writing result
 -tp,--tagprops <arg>                               tag props to scan out, split by comma
 -u,--user <arg>                                    target NebulaGraph user, used for writing result
```

提交命令
```
spark-submit \
--master "spark://192.168.15.2:1077" \
--class com.vesoft.nebula.examples.connector.AggregateData \
nebula-poc-aggregate.jar \
-m "192.168.15.5:9559" -space "pocSpace" -l 10 -p 10 -t "pocTag" -e "trans" -tp "type,prop1" -ep "amount,buyer,saler,market,p1,p2" -efields "buyer,saler" -tfield "_vertexId" -reduceField "amount" -tagTypeField "type" -targetSpace "pocResult" -targetEdge "result" -targetGraphAddr "192.168.15.5:9669,192.168.15.6:9669,192.168.15.7:9669" -targetMetaAddr "192.168.15.5:9559"  -targetEdgePropField "count" -b 3  -timeout 30000 -u root -passwd nebula
```
