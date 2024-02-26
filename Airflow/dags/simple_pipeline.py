# from airflow import DAG
# from airflow.operators.python import PythonOperator,BranchPythonOperator
# from airflow.operators.bash import BashOperator
# from datetime import datetime,timedelta
# from confluent_kafka import Producer,Consumer

# default_args = {
#     'owner':'Airflow',
#     'retries':1,
#     'retry_delay':timedelta(minutes=2),
#     'start_date':datetime(2023,12,13)
# }

# def read_file_from_local(**context):
#     with open('/opt/airflow/files/a.txt','r') as file:
#         file_contents = file.read()
#         file.close()
#     context['ti'].xcom_push(key='file_data',value = file_contents)
#     return file_contents

# def push_file_data_to_kafka_topics(**context):
#     file_contents = context['ti'].xcom_pull(key='file_data')

#     # Set up Kafka producer configuration
#     conf = {
#         'bootstrap.servers': 'localhost:9092',  # Replace with your Kafka bootstrap servers
#     }

#     # Create Kafka producer instance
#     producer = Producer(conf)

#     # Define the Kafka topic to which you want to send messages
#     topic = 'test'

#     # Produce a message to the Kafka topic
#     message_key = 'file_data'  # Replace with your desired message key
#     message_value = file_contents  # Replace with your actual message

#     # Produce the message to the Kafka topic
#     producer.produce(topic, key=message_key, value=message_value)

#     # Flush the producer to ensure all messages are sent
#     producer.flush()

# def fetch_data_from_kafka_topics(**context):
#     c = Consumer({
#     'bootstrap.servers': 'localhost:9092',
#     'group.id': 'test',
#         'auto.offset.reset': 'earliest'
#     })

#     c.subscribe(['test'])

#     while True:
#         msg = c.poll(1.0)

#         if msg is None:
#             continue
#         if msg.error():
#             print("Consumer error: {}".format(msg.error()))
#             continue

#         print('Received message: {}'.format(msg.value().decode('utf-8')))
#         break

#     c.close()




# with DAG(
#     dag_id='count_words_pipeline',
#     default_args=default_args,
#     description='A simple pipeline to count words',
#     schedule_interval=timedelta(days=1),
#     tags=['first_pipeline'],
#     catchup = False
# ) as dag:
    
#     read_file = PythonOperator(
#         task_id = 'read_file',
#         python_callable = read_file_from_local,
#         provide_context = True
#     )

#     push_data_to_kafka_topic = PythonOperator(
#         task_id = 'push_data_to_kafka_topic',
#         python_callable = push_file_data_to_kafka_topics,
#         provide_context = True
#     )

#     fetch_data_to_kafka_topic = PythonOperator(
#         task_id = 'fetch_data_to_kafka_topic',
#         python_callable = fetch_data_from_kafka_topics,
#         provide_context = True
#     )

#     read_file >> push_data_to_kafka_topic >> fetch_data_to_kafka_topic