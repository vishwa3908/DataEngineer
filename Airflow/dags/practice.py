from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import BranchPythonOperator,PythonOperator
from airflow.sensors.filesystem import FileSensor
from datetime import datetime,timedelta
import random,string
default_args = {
    'owner':'Airflow',
    'retries':1,
    'retry_delay':timedelta(minutes=2),
    'start_date':datetime(2023,12,13)
}

def generate_string(*op_args,**context):
    length = op_args[0]
    result = ''.join((random.choice(string.ascii_lowercase) for x in range(length)))
    context['ti'].xcom_push(key = 'random_string',value = result)

def find_vowel_count(**context):
    gen_string = context['ti'].xcom_pull(task_ids ='generate_random_string',key ='random_string')
    vowel_count = 0
    const_count = 0
    for character in gen_string:
        if character in ['a','e','i','o','u','A','E','I','O','U']:
            vowel_count+=1
        else:
            const_count+=1
    context['ti'].xcom_push(key='vowel_count',value=vowel_count)
    context['ti'].xcom_push(key='const_count',value=const_count)

def check_vowel_const_count(**context):
    vowel_c = context['ti'].xcom_pull(key='vowel_count')
    const_c  = context['ti'].xcom_pull(key='const_count')
    if vowel_c >= 2:
        return 'positive'
    else:
        return 'negative'
    
def fact_5():
    res = 1
    num = 5
    while num >0:
        res*=num
        num-=1

def fact_6():
    res = 1
    num = 6
    while num >0:
        res*=num
        num-=1

    

with DAG(
    dag_id = 'Test_Operators',
    default_args=default_args,
    description='Dag to Practice Operators',
    schedule_interval=timedelta(days=1),
    catchup = False) as dag:
    generate_random_string = PythonOperator(
        task_id = 'generate_random_string',
        python_callable = generate_string,
        op_args = [11],
        provide_context = True
    )

    find_vowel_const_count = PythonOperator(
        task_id='find_vowel_const_count',
        python_callable = find_vowel_count,
        provide_context = True,
        doc_md = 'Tasks to find vowel and consonant count in generated string'
    )

    sense_string_file = FileSensor(
        task_id = 'sense_string_file',
        fs_conn_id = 'filetest',
        filepath = 'a.txt',
        poke_interval = 10,
        timeout = timedelta(minutes=2),
        mode='poke',
        soft_fail = False
    )

    check_count = BranchPythonOperator(
        task_id = 'check_count',
        python_callable = check_vowel_const_count
    )

    positive = PythonOperator(
        task_id='positive',
        python_callable = fact_5
    )
    negative = PythonOperator(
        task_id = 'negative',
        python_callable = fact_6
    )

    generate_random_string >>find_vowel_const_count >> check_count >> [positive,negative] >> sense_string_file




