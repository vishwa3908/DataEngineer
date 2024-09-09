# from datetime import datetime,timedelta
# from airflow import DAG
# from airflow.operators.python import PythonOperator
# from airflow.providers.amazon.aws.hooks.s3 import S3Hook

# default_args = {
#     'owner':'Vishwaranjan',
#     'retries':1,
#     'retry_delay':timedelta(minutes=2),
#     'start_date':datetime(2023,12,13)
# }

# def welcome():
#     print("hello s3")

# def upload_to_s3(filename:str,key:str,bucket_name:str):
#     hook = S3Hook('s3-conn')
#     hook.load_file(filename=filename,key=key,bucket_name=bucket_name)
     
# def download_from_s3(key, bucket_name, local_path):
#     hook = S3Hook('s3-conn')
#     hook.download_file(key=key,bucket_name=bucket_name,local_path=local_path)


# with DAG(
#     dag_id='test_s3_bucket',
#     default_args=default_args,
#     description='A simple pipeline to test s3 conn',
#     schedule_interval=timedelta(days=1),
#     tags=['first_pipeline'],
#     catchup = False
# ) as dag:
    
#     t1 = PythonOperator(
#         task_id="welcome",
#         python_callable=welcome
#         )
    
#     task_upload_to_s3 = PythonOperator(
#         task_id= 'upload_to_s3',
#         python_callable = upload_to_s3,
#         op_kwargs={
#             'filename': '/opt/airflow/files/IPL_PLAYERS.csv',
#             'key':'IPL_players.csv',
#             'bucket_name':'testbucket-3908'
#         }
#     )

#     download_from_s3 = PythonOperator(
#         task_id= 'download_from_s3',
#         python_callable=download_from_s3,
#         op_kwargs={
#             'key':'IPL_players.csv',
#             'bucket_name':'testbucket-3908',
#             'local_path':'/opt/airflow/files/s3/'
#         }
#     )
    
#     t1 >> task_upload_to_s3 >> download_from_s3


from datetime import timedelta, datetime
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

# Spark job Python script as a string
spark_job = """
from pyspark.sql import SparkSession

# Create Spark session
spark = SparkSession.builder \
    .appName("SimpleSparkJob") \
    .getOrCreate()

# Create a simple DataFrame with some values
data = [("Alice", 1), ("Bob", 2), ("Catherine", 3), ("David", 4)]
columns = ["Name", "ID"]

df = spark.createDataFrame(data, columns)

# Perform a simple operation: Show the DataFrame
df.show()

# Another simple operation: Filter rows where ID is greater than 2
filtered_df = df.filter(df.ID > 2)

# Show the filtered DataFrame
filtered_df.show()

# Stop the Spark session
spark.stop()
"""

# Write the Spark job to a local file
with open('spark_job.py', 'w') as f:
    f.write(spark_job)

# Define default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
with DAG(
    'spark_job_dag',
    default_args=default_args,
    description='A simple Spark job DAG',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 9, 1),
    catchup=False,
) as dag:

    # Task: Run Spark job using SparkSubmitOperator
    run_spark_job = SparkSubmitOperator(
        task_id='run_spark_job',
        application='spark_job.py',  # Path to the Spark job script
        conn_id='test',  # Connection ID for Spark
        conf={'spark.master': 'spark://spark-master:7077'},  # Spark master URL
        executor_cores=2,
        executor_memory='2g',
        name='spark_job',
        verbose=True
    )

    run_spark_job
