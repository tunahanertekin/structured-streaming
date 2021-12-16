kill -9 $(lsof -t -i:4040)
cd stream_handler
sbt package
spark-submit --class StreamHandler --master "local[*]" --packages "org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.5" target/scala-2.11/streamhandler_2.11-1.0.jar