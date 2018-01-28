mvn clean install -DskipTests
aws s3 cp ./target/es-spark-offline-index-0.0.1-SNAPSHOT.jar <s3://jar location>
