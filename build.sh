cd example
mvn clean package -Dmaven.test.skip=true
mv target/example-3.0-SNAPSHOT-jar-with-dependencies.jar ../nebula-poc-aggregate.jar
