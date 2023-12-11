from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime,timedelta
from random import randint
from airflow.models import XCom

def generate_random_num(**op_kwargs):
    random_num = op_kwargs['num']
    return random_num
    

def odd_even(*op_args):
    data = op_args[0]
    # d = XCom.get_many(
    #     execution_date = context['execution_date'],
    #     dag_ids = context['dag'].dag_id,
    #     include_prior_dates = True
    # )
    
    if data %2 == 0:
        return 'Even'
    else:
        return 'Odd'

with DAG(
    dag_id = 'practice_dag',
    description='A Dag to practice different operators',
    schedule_interval=timedelta(minutes=51),
    default_args={
        'owner':'airflow',
        'retries':1,
        'retry_delay':timedelta(minutes=5),
        'start_date':datetime(2023,12,10)
                },
    catchup=False
    ) as dag:

    welcome = BashOperator(
        task_id = 'welcome',
        bash_command = 'echo "Welcome to Airflow"'
    )

    python_task = PythonOperator(
        task_id="python_task",
        python_callable=generate_random_num,
        op_kwargs = {
            'num':333
        }
    )

    bash_task = BashOperator(
        task_id="bash_task",
        bash_command='echo "Hi from bash operator"'
    )
    check_even_odd = PythonOperator(
        task_id = 'check_even_odd',
        python_callable = odd_even,
        op_args = [21]
    )

    
welcome >> bash_task >> python_task >> check_even_odd