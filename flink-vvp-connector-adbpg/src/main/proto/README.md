# How to generate java grpc files
```bash
# edit proto file and replace java_package name from "api" to "your project package name"
vi adbpgss.proto
protoc --java_out=. --grpc-java_out=. --proto_path=. adbpgss.proto
mv org/apache/flink/connector/jdbc/table/sink/api java/src/org/apache/flink/connector/jdbc/table/sink/
## move api/ to your java project
rm -rf org
```