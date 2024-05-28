from confluent_kafka import Consumer, KafkaError

# Kafka consumer configuration
config = {
    'bootstrap.servers': 'localhost:9092',  # Kafka broker(s)
    'group.id': 'test_group',  # Consumer group ID
    'auto.offset.reset': 'earliest'  # Start reading from the earliest offset
    # Add more configuration options as needed
}

# Create a Kafka consumer instance
consumer = Consumer(config)

# Subscribe to the topic
topic = 'pysparkproject'
consumer.subscribe([topic])

# Consume messages
try:
    while True:
        message = consumer.poll(1.0)  # Wait for 1 second for new messages
        if message is None:
            continue
        if message.error():
            if message.error().code() == KafkaError._PARTITION_EOF:
                # End of partition
                print(f'End of partition reached: {message.topic()} [{message.partition()}] at offset {message.offset()}')
            elif message.error():
                # Error
                print(f'Error: {message.error()}')
        else:
            # Print the message key and value
            print(f'Received message: key={message.key().decode("utf-8")}, value={message.value().decode("utf-8")}')
except KeyboardInterrupt:
    pass
finally:
    # Close the consumer
    consumer.close()
