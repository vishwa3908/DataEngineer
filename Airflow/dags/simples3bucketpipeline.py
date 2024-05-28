from datetime import datetime,timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

default_args = {
    'owner':'Vishwaranjan',
    'retries':1,
    'retry_delay':timedelta(minutes=2),
    'start_date':datetime(2023,12,13)
}

def welcome():
    print("hello s3")

def upload_to_s3(filename:str,key:str,bucket_name:str):
    hook = S3Hook('s3-conn')
    hook.load_file(filename=filename,key=key,bucket_name=bucket_name)
     
def download_from_s3(key, bucket_name, local_path):
    hook = S3Hook('s3-conn')
    hook.download_file(key=key,bucket_name=bucket_name,local_path=local_path)


with DAG(
    dag_id='test_s3_bucket',
    default_args=default_args,
    description='A simple pipeline to test s3 conn',
    schedule_interval=timedelta(days=1),
    tags=['first_pipeline'],
    catchup = False
) as dag:
    
    t1 = PythonOperator(
        task_id="welcome",
        python_callable=welcome
        )
    
    task_upload_to_s3 = PythonOperator(
        task_id= 'upload_to_s3',
        python_callable = upload_to_s3,
        op_kwargs={
            'filename': '/opt/airflow/files/IPL_PLAYERS.csv',
            'key':'IPL_players.csv',
            'bucket_name':'testbucket-3908'
        }
    )

    download_from_s3 = PythonOperator(
        task_id= 'download_from_s3',
        python_callable=download_from_s3,
        op_kwargs={
            'key':'IPL_players.csv',
            'bucket_name':'testbucket-3908',
            'local_path':'/opt/airflow/files/s3/'
        }
    )
    
    t1 >> task_upload_to_s3 >> download_from_s3