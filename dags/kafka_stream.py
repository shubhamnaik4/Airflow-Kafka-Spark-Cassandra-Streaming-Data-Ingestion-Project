import uuid
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator

# Default arguments for the DAG
default_args = {
    'owner': 'shubham_naik',  # Updated owner name
    'start_date': datetime(2025, 1, 15, 10, 00),  
}

# Function to fetch random user data from the API
def fetch_random_user_data():
    import requests

    # Sending GET request to randomuser.me API to get random user data
    response = requests.get("https://randomuser.me/api/")
    data = response.json()
    
    # Extracting the first result from the API response
    user_data = data['results'][0]

    return user_data

# Function to format the raw user data
def transform_user_data(user_data):
    formatted_data = {}
    
    # Extracting and formatting location data
    location = user_data['location']
    
    # Generating a unique ID for each user
    formatted_data['user_id'] = uuid.uuid4()
    formatted_data['first_name'] = user_data['name']['first']
    formatted_data['last_name'] = user_data['name']['last']
    formatted_data['gender'] = user_data['gender']
    formatted_data['address'] = f"{str(location['street']['number'])} {location['street']['name']}, " \
                                f"{location['city']}, {location['state']}, {location['country']}"
    formatted_data['postal_code'] = location['postcode']
    formatted_data['email'] = user_data['email']
    formatted_data['username'] = user_data['login']['username']
    formatted_data['date_of_birth'] = user_data['dob']['date']
    formatted_data['registration_date'] = user_data['registered']['date']
    formatted_data['phone'] = user_data['phone']
    formatted_data['profile_picture'] = user_data['picture']['medium']

    return formatted_data

# Function to stream the data to Kafka
def stream_data_to_kafka():
    import json
    from kafka import KafkaProducer
    import time
    import logging

    # Initialize Kafka producer to send data to the Kafka topic 'users_created'
    producer = KafkaProducer(bootstrap_servers=['broker:29092'], max_block_ms=5000)

    # Get the current time for 1-minute data streaming duration
    start_time = time.time()

    while True:
        # Stop the loop after 1 minute
        if time.time() > start_time + 60:
            break
        try:
            # Fetching and transforming the data
            user_data = fetch_random_user_data()
            transformed_data = transform_user_data(user_data)

            # Sending the formatted data to the Kafka topic
            producer.send('users_created', json.dumps(transformed_data).encode('utf-8'))
        except Exception as e:
            # Logging any error that occurs during data streaming
            logging.error(f'Error occurred: {e}')
            continue

# Define the Airflow DAG
with DAG('user_automation',  # Name of the DAG
         default_args=default_args,  # Default arguments for the DAG
         schedule_interval='@daily',  # Schedule interval for the DAG
         catchup=False) as dag:

    # Create the task to stream data from the API
    streaming_task = PythonOperator(
        task_id='stream_random_user_data',
        python_callable=stream_data_to_kafka  # Function to be called by the task
    )
