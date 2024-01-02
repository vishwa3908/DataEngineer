from confluent_kafka import Producer



conf = {'bootstrap.servers': 'localhost:9092'}

producer = Producer(conf)
producer.produce('test', key="key", value="hello")
producer.flush()