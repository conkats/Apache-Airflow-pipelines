from datetime import datetime, timedelta
from airflow.utils.dates import days_ago

from airflow import DAG

from airflow.operators.python import PythonOperator

default_args = {
    'owner' : 'loonycorn'
}

def print_function():
    print("The simplest possible Python operator!")

# instantiate the DAG object
with DAG(
    dag_id = 'execute_python_operators',
    description = 'Python operators in DAGs',
    default_args = default_args,
    start_date = days_ago(1),#start 1 day ago
    schedule_interval = '@daily', # run on a daily basis
    tags = ['simple', 'python'] # tags are used to categorize the DAGs
) as dag:
    # task -python operator and callable executor
    task = PythonOperator(
        task_id = 'python_task',
        python_callable = print_function
    )

task    
