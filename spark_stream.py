import logging
from cassandra.cluster import Cluster
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType

# Function to create a keyspace in Cassandra
def create_keyspace(cassandra_session):
    cassandra_session.execute("""
        CREATE KEYSPACE IF NOT EXISTS user_data_streams
        WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'};
    """)

    logging.info("Keyspace 'user_data_streams' created successfully!")


# Function to create a table in Cassandra to store user data
def create_table(cassandra_session):
    cassandra_session.execute("""
    CREATE TABLE IF NOT EXISTS user_data_streams.users (
        user_id UUID PRIMARY KEY,
        first_name TEXT,
        last_name TEXT,
        gender TEXT,
        address TEXT,
        postal_code TEXT,
        email TEXT,
        username TEXT,
        registration_date TEXT,
        phone TEXT,
        profile_picture TEXT);
    """)

    logging.info("Table 'users' created successfully!")


# Function to insert data into Cassandra
def insert_data_to_cassandra(cassandra_session, **user_data):
    logging.info("Inserting user data into Cassandra...")

    try:
        cassandra_session.execute("""
            INSERT INTO user_data_streams.users(user_id, first_name, last_name, gender, address, 
                postal_code, email, username, registration_date, phone, profile_picture)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, (user_data['user_id'], user_data['first_name'], user_data['last_name'], user_data['gender'],
              user_data['address'], user_data['postal_code'], user_data['email'], user_data['username'],
              user_data['registration_date'], user_data['phone'], user_data['profile_picture']))
        logging.info(f"Data inserted for user: {user_data['first_name']} {user_data['last_name']}")

    except Exception as e:
        logging.error(f"Error inserting data into Cassandra: {e}")


# Function to create a Spark session and connect to Spark
def create_spark_session():
    spark_session = None

    try:
        spark_session = SparkSession.builder \
            .appName('UserDataStreaming') \
            .config('spark.jars.packages', "com.datastax.spark:spark-cassandra-connector_2.13:3.4.1,"
                                           "org.apache.spark:spark-sql-kafka-0-10_2.13:3.4.1") \
            .config('spark.cassandra.connection.host', 'localhost') \
            .getOrCreate()

        spark_session.sparkContext.setLogLevel("ERROR")
        logging.info("Spark session created successfully!")
    except Exception as e:
        logging.error(f"Failed to create Spark session: {e}")

    return spark_session


# Function to connect to Kafka and read data streams
def connect_to_kafka(spark_session):
    try:
        kafka_df = spark_session.readStream \
            .format('kafka') \
            .option('kafka.bootstrap.servers', 'localhost:9092') \
            .option('subscribe', 'users_created') \
            .option('startingOffsets', 'earliest') \
            .load()

        logging.info("Kafka DataFrame created successfully")
        return kafka_df
    except Exception as e:
        logging.error(f"Failed to create Kafka DataFrame: {e}")
        return None


# Function to create a connection to the Cassandra cluster
def create_cassandra_connection():
    try:
        # Connecting to the Cassandra cluster
        cluster = Cluster(['localhost'])
        cassandra_session = cluster.connect()
        logging.info("Connected to Cassandra cluster successfully!")
        return cassandra_session
    except Exception as e:
        logging.error(f"Failed to connect to Cassandra: {e}")
        return None


# Function to convert the incoming Kafka data into a structured format using a schema
def create_streaming_df_from_kafka(kafka_df):
    schema = StructType([
        StructField("user_id", StringType(), False),
        StructField("first_name", StringType(), False),
        StructField("last_name", StringType(), False),
        StructField("gender", StringType(), False),
        StructField("address", StringType(), False),
        StructField("postal_code", StringType(), False),
        StructField("email", StringType(), False),
        StructField("username", StringType(), False),
        StructField("registration_date", StringType(), False),
        StructField("phone", StringType(), False),
        StructField("profile_picture", StringType(), False)
    ])

    # Transform the Kafka stream into the desired format using the schema
    structured_df = kafka_df.selectExpr("CAST(value AS STRING)") \
        .select(from_json(col('value'), schema).alias('data')).select("data.*")

    return structured_df


# Main function to run the entire data pipeline
if __name__ == "__main__":
    # Create Spark session
    spark_session = create_spark_session()

    if spark_session:
        # Connect to Kafka and read the data stream
        kafka_df = connect_to_kafka(spark_session)
        
        if kafka_df:
            # Create a structured DataFrame from Kafka stream
            streaming_df = create_streaming_df_from_kafka(kafka_df)
            
            # Connect to Cassandra
            cassandra_session = create_cassandra_connection()

            if cassandra_session:
                # Create Cassandra keyspace and table
                create_keyspace(cassandra_session)
                create_table(cassandra_session)

                logging.info("Starting data streaming...")

                # Write the streaming data into Cassandra table
                streaming_query = (streaming_df.writeStream
                                   .format("org.apache.spark.sql.cassandra")
                                   .option('checkpointLocation', '/tmp/checkpoint')
                                   .option('keyspace', 'user_data_streams')
                                   .option('table', 'users')
                                   .start())

                # Wait for the streaming query to terminate
                streaming_query.awaitTermination()
