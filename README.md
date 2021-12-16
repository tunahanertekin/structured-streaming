# Structured Streaming with Kafka and Spark
A mock fleet system that uses Kafka topics to communicate and analyzing the stream data with Spark Structured Streaming. Developed for TOBB ETÜ BIL 401 class project in Fall 2021.

## Prerequisites
- Apache Kafka v2.4.x ([kafka_2.11-2.4.1.tgz](https://archive.apache.org/dist/kafka/2.4.1/kafka_2.11-2.4.1.tgz))
- Apache Spark v2.4.5 ([spark-2.4.5-bin-hadoop2.7.tgz](https://archive.apache.org/dist/spark/spark-2.4.5/spark-2.4.5-bin-hadoop2.7.tgz))
- Scala v2.11.12
- Python v3.6 or higher

Clone the repository.

`git clone https://github.com/tunahanertekin/structured-streaming`

`cd structural-streaming`

Open five executive terminals and run the following commands at each respectively from project root.

- Build Spark project written in Scala, using sbt. Then run with `spark-submit`.

`scripts/stream_handler.sh`

- Run Spark consumer. (A Kafka consumer which reads stream comes from Spark, as dataframe)

`scripts/spark_consumer.sh`

- Run result collector. (A Kafka consumer which reads stream comes from fleet members)

`scripts/result_collector.sh`

- Run fleet. (Represents members of fleet. It's subscribed to fleet manager and publishes to result collector)

`scripts/fleet.sh`

- Run fleet manager. (A Kafka producer which publishes tasks to fleet members)

`scripts/fleet_manager.sh`
