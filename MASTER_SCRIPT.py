import os
import requests
import json
from requests import Session
import time
from confluent_kafka.admin import AdminClient, NewTopic
from confluent_kafka import Producer

# All Variables needed for the MASTER_SCRIPT
SCHEMA_REGISTRY_URL = "http://svc-schemaregistry-headless.starlake-kafka:8081"
SCHEMA_FOLDER = "./schema"

config_data = {
    "bootstrap_servers": "svc-kafka-headless.starlake-kafka:9092",
    "topics": [
        {"topic": "sheeps", "num_partitions": 1, "replication_factor": 1, "config": {"cleanup.policy": "delete", "retention.ms": "86400000", "confluent.stray.log.max.deletions.per.run": 72}},
        {"topic": "whales", "num_partitions": 1, "replication_factor": 1, "config": {"cleanup.policy": "delete", "retention.ms": "86400000"}},
        {"topic": "birds", "num_partitions": 1, "replication_factor": 1, "config": {"cleanup.policy": "delete", "retention.ms": "86400000"}},
        {"topic": "sheep_names_farm", "num_partitions": 1, "replication_factor": 1, "config": {"cleanup.policy": "delete", "retention.ms": "86400000"}},
        {"topic": "whale_names_farm", "num_partitions": 1, "replication_factor": 1, "config": {"cleanup.policy": "delete", "retention.ms": "86400000"}},
        {"topic": "bird_names_farm", "num_partitions": 1, "replication_factor": 1, "config": {"cleanup.policy": "delete", "retention.ms": "86400000"}}
    ]
}

server_url = "http://svc-kafka-ksqldb-server-headless.starlake-kafka:8088"
stream_folder = "sql_queries_stream"
table_folder = "sql_queries_table"
combined_folder = "sql_queries_combined"
all_folder = "all_stream_combined"

bootstrap_servers = "svc-kafka-headless.starlake-kafka:9092"
folder = "animal_mapping"

CONNECT_URL = "http://svc-kafkaconnect-headless.starlake-kafka:8083/connectors"
JSON_FOLDER = "json_config_files"

# Create Schema
# Check if the schema folder exists
if not os.path.isdir(SCHEMA_FOLDER):
    print("Error: Schema folder '{}' not found.".format(SCHEMA_FOLDER))
    sys.exit(1)

# Iterate over each file in the schema folder
for filename in os.listdir(SCHEMA_FOLDER):
    schema_file = os.path.join(SCHEMA_FOLDER, filename)

    # Check if the current item is a file
    if os.path.isfile(schema_file):
        # Read Avro schema from file
        with open(schema_file, 'r') as f:
            AVRO_SCHEMA = f.read()
            print(AVRO_SCHEMA)

        # Extract subject from file name (assuming file name is subject.avsc)
        subject = os.path.splitext(filename)[0] + "-value"

        # Define HTTP request URL for posting the schema
        REQUEST_URL = "{}/subjects/{}/versions".format(SCHEMA_REGISTRY_URL, subject)

        # Post the Avro schema to Schema Registry
        response = requests.post(REQUEST_URL, headers={"Content-Type": "application/vnd.schemaregistry.v1+json"}, data=AVRO_SCHEMA)
        print(response.text)
        
# Create Topic
# Extract bootstrap servers from config
bootstrap_servers = config_data["bootstrap_servers"]

# Create AdminClient instance
admin_client = AdminClient({'bootstrap.servers': bootstrap_servers})

# Extract topics from config
topics_config = config_data["topics"]

# Create topics
topics = [NewTopic(**config) for config in topics_config]
future_results = admin_client.create_topics(topics)

# Wait for topic creation to finish
admin_client.poll(10)

# Check for any errors during topic creation
for topic, future in future_results.items():
    try:
        future.result()  # Wait for the topic to be created
        print(f"Topic {topic} created successfully.")
    except Exception as e:
        print(f"Failed to create topic {topic}: {e}")
        

# Run SQL queries on KSQLDB
ksql_client = Session()
ksql_client.headers.update({'Content-Type': 'application/vnd.ksql.v1+json'})
def create_ksql_queries(sql_folder):
    for filename in os.listdir(sql_folder):
        if filename.endswith(".sql"):
            file_path = os.path.join(sql_folder, filename)
            
            # Read SQL command from file
            with open(file_path, "r") as file:
                sql_command = file.read()

            # Execute SQL command
            response = ksql_client.post(f"{server_url}/ksql", json={'ksql': sql_command})
            
            # Check response status code
            response.raise_for_status()

            # Print response
            print(f"Response for {filename}: {response.json()}")
            
create_ksql_queries(stream_folder)
create_ksql_queries(table_folder)
create_ksql_queries(combined_folder)
create_ksql_queries(all_folder)

# Produce data to kafka from CSV
def read_files_in_folder(folder_path):
    # Check if the folder exists
    if not os.path.exists(folder_path):
        print(f"Folder '{folder_path}' does not exist.")
    
    # List all files in the folder
    files = os.listdir(folder_path)
    return files

def delivery_report(err, msg):
    if err is not None:
        print('Message delivery failed: {}'.format(err))
    else:
        print('Message delivered to {} [{}]'.format(msg.topic(), msg.partition()))
        
# Read the files in the folder
files = read_files_in_folder(folder)
for filename in files:
    file_path = os.path.join(folder, filename)
    with open(file_path, 'r') as in_file:
        in_file.readline()
        lst = in_file.readlines()

    # Configuration for Kafka producer
    conf = {
        'bootstrap.servers': bootstrap_servers,
        'client.id': filename
    }

    # Create Kafka producer instance
    producer = Producer(conf)

    # Produce a message to Kafka topic
    for index in range(len(lst)):
        curr = lst[index].split(',')
        key = curr[0]  # Optional: You can set a key for the message
        value = curr[1]

        producer.produce(filename.rstrip(".csv"), key=key, value=value, callback=delivery_report)

    # Wait for any outstanding messages to be delivered and delivery reports received
        producer.flush()
        time.sleep(0.1)
        
        
# Set up the connectors
# Check if the JSON folder exists
if not os.path.isdir(JSON_FOLDER):
    print(f"Error: JSON folder '{JSON_FOLDER}' not found.")
    sys.exit(1)

# Iterate over each file in the JSON folder
for filename in os.listdir(JSON_FOLDER):
    json_file = os.path.join(JSON_FOLDER, filename)

    # Check if the current item is a file and ends with '.json'
    if os.path.isfile(json_file) and filename.endswith('.json'):
        # Send the file to Kafka Connect
        with open(json_file, 'rb') as file:
            response = requests.post(CONNECT_URL, headers={"Content-Type": "application/json"}, data=file)
            print(response.text)
