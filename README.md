# • Airflow-Kafka-Spark-Cassandra Streaming Data Ingestion Project:


## Introduction

Designed and implemented a data pipeline to process and ingest random user data from the randomuser.me API into PostgreSQL. Utilized Apache Kafka and Zookeeper for streaming data to Apache Spark for processing, with Cassandra as the storage layer.Orchestrated workflows using Apache Airflow and managed Kafka streams with Control Center and Schema Registry.


The project is designed with the following components:

- **Data Source**: We use `randomuser.me` API to generate random user data for our pipeline.
- **Apache Airflow**: Orchestrates the pipeline and stores the fetched data in a PostgreSQL database.
- **Apache Kafka and Zookeeper**: Facilitates streaming data from PostgreSQL to the processing engine.
- **Control Center and Schema Registry**: Ensures monitoring and schema management of the Kafka streams.
- **Apache Spark**: Processes the data through its distributed master and worker nodes.
- **Cassandra**: Stores the processed data for future analysis.


## Technologies

- Apache Airflow
- Python
- Apache Kafka
- Apache Zookeeper
- Apache Spark
- Cassandra
- PostgreSQL
- Docker

