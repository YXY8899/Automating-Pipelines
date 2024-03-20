import os
import random
import time
from datetime import datetime

import psycopg2

# Environment variables for database configuration
farm = os.environ.get("FARM", "Farm1")
table = os.environ.get("TABLE", "sheeps")

host = os.environ.get("POSTGRES_HOST", "svc-postgres-headless.starlake-kafka")
port = os.environ.get("POSTGRES_PORT", "5432")
database = os.environ.get("POSTGRES_DB", "postgres")
username = os.environ.get("POSTGRES_USER", "postgres")
password = os.environ.get("POSTGRES_PASSWORD", "plschangemepls")

# Define the delay range from environment variable in seconds (e.g., 0.1 to 1)
delay_range = os.environ.get("DELAY_RANGE", "1-10").split("-")
min_delay = float(delay_range[0])
max_delay = float(delay_range[1])

# Create a connection string
conn_str = f"host={host} port={port} dbname={database} user={username} password={password}"
print(conn_str)

# Establish connection to PostgreSQL database
connection = psycopg2.connect(conn_str)
cursor = connection.cursor()

# Create a new schema and grant permissions if it doesn't exist
GRANT_ALL_PRIVILEGES = f"GRANT CREATE ON DATABASE postgres TO {username};"

cursor.execute(GRANT_ALL_PRIVILEGES)
connection.commit()

# Check if the table exists in the database
table_exists_query = f"SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = '{table}');"
cursor.execute(table_exists_query)
table_exists = cursor.fetchone()[0]

if not table_exists:
    # Create a table for sheep locations if it doesn't exist
    CREATE_TABLE_QUERY = f'''
        CREATE TABLE {table} (
            id SERIAL PRIMARY KEY,
            farm VARCHAR(10),
            sheep_id INT,
            longitude FLOAT,
            latitude FLOAT,
            date_time TIMESTAMP,
            file_path VARCHAR(50)
        );
    '''
    cursor.execute(CREATE_TABLE_QUERY)
    connection.commit()

# Generate and stream sheep location data at 1Hz
try:
    while True:
        # Generate random data for sheep location
        print('Sheep location sent..')
        sheep_id = random.randint(1, 35)
        longitude = random.uniform(-180, 180)
        latitude = random.uniform(-90, 90)
        date_time = datetime.now()
        file_path = f"./image/{sheep_id}.png"

        # SQL query to insert the sheep location data
        QUERY = f"INSERT INTO {table} (farm, sheep_id, longitude, latitude, date_time, file_path) \
            VALUES (%s, %s, %s, %s, %s, %s)"
        values = (farm, sheep_id, longitude, latitude, date_time, file_path)

        # Execute the SQL query
        cursor.execute(QUERY, values)
        connection.commit()

        # Generate a random delay within the specified range
        random_delay = random.uniform(min_delay, max_delay)
        time.sleep(random_delay)
finally:
    cursor.close()
    connection.close()