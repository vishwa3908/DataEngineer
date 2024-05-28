from confluent_kafka import Producer
import pandas as pd
import json
import time
# Kafka producer configuration
config = {
    'bootstrap.servers': 'localhost:9092',  # Kafka broker(s)
    # Add more configuration options as needed
}

# Create a Kafka producer instance
producer = Producer(config)

# Define the topic to produce messages to
topic = 'pysparkproject'

# Produce messages
key = 'bio'

# Read the CSV file into a Pandas DataFrame
customer_df = pd.read_csv("/Users/prabhatkumar/Desktop/DataEngineer/PySpark/files/csv/customers.csv")

# Define the number of rows per chunk
chunk_size = 10

# Create an empty list to store dictionaries
dict_list = []

# Iterate over the DataFrame in chunks of size chunk_size
for i in range(0, len(customer_df), chunk_size):
    # Slice the DataFrame into a chunk
    chunk_df = customer_df.iloc[i:i+chunk_size]
    
    # Convert the chunk DataFrame to a dictionary
    chunk_dict = chunk_df.to_dict(orient='records')
    
    # Append the chunk dictionary to the list
    dict_list.append(chunk_dict)

# Print the list of dictionaries
for i, chunk_dict in enumerate(dict_list):
    print(f"Dictionary for chunk {i+1}:")
    for row in chunk_dict:
        
    # Send each chunk dictionary to the Kafka

        message = json.dumps(row)
        producer.produce(topic,key = key,value= message)
        print(f'Produced: {message}')
        

        # Flush the producer to ensure all messages are delivered
        producer.flush()
    time.sleep(10)
