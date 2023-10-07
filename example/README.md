# Example for nebula-spark-connector

## How to compile

for spark2.2:
```agsl
cd example
mvn clean package -Dmaven.test.skip=true -Pspark2.2
```

for spark2.4:
```agsl
cd example
mvn clean package -Dmaven.test.skip=true -Pspark2.4
```

for spark3.x
```agsl
cd example
mvn clean package -Dmaven.test.skip=true -Pspark3.0
```

## export Nebula to File
Support export Nebula data to HDFS or S3 or OSS with format CSV, JSON, PARQUET.
You can just config your nebula meta address, space, and the target file path to export Nebula data to file.

### how to export
```agsl
spark-submit \
--master local \
--class com.vesoft.nebula.examples.connector.Nebula2File \
example-3.0-SNAPSHOT-jar-with-dependencies.jar \
-sourceMeta 127.0.0.1:9559 \
-sourceSpace test \
-targetFilePath file:///tmp/export -targetFileFormat csv
```

Then the Nebula data will be export to /tmp/export, data in different tags will be located in different directories.

For more useage, please with the command to show more config options:
```agsl
spark-submit --master local --class com.vesoft.nebula.examples.connector.Nebula2File example-3.0-SNAPSHOT-jar-with-dependencies.jar
```

```agsl
usage: >>>> options
 -accessKey,--accessKey <arg>                 access key for oss or s3
 -endpoint,--endpoint <arg>                   endpoint for oss or s3
 -excludeEdges,--excludeEdges <arg>           filter out these edges, separate with `,`
 -excludeTags,--excludeTags <arg>             filter out these tags, separate with `,`
 -includeTag,--includeTag <arg>               only migrate the specific tag
 -limit,--limit <arg>                         records for one reading request for reading
 -noFields,--noFields <arg>                   no property field for reading, true or false
 -secretKey,--secretKey <arg>                 secret key for oss or s3
 -sourceMeta,--sourceMetaAddress <arg>        source nebulagraph metad address
 -sourceSpace,--sourceSpace <arg>             source nebulagraph space name
 -targetFileFormat,--targetFileFormat <arg>   target file format to save Nebula data, support csv, parquet, json
 -targetFilePath,--targetFilePath <arg>       target file path to save Nebula data
 -targetFileSystem,--targetFileSystem <arg>   target file system to save Nebula data, support hdfs,oss,s3
```
