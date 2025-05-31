import time

from datetime import datetime, timedelta
from airflow.utils.dates import days_ago

from airflow import DAG

from airflow.operators.python import PythonOperator

default_args = {
    'owner' : 'loonycorn'
}
#Python functions that do work of the tasks
def greet_hello(name):
    print("Hello, {name}!".format(name=name))

def greet_hello_with_city(name, city):
    print("Hello, {name} from {city}".format(name=name, city=city))

#instantiate the DAG object with the default arguments
with DAG(
    dag_id = 'execute_python_operators',
    description = 'Python operators in DAGs with parameters',
    default_args = default_args,
    start_date = days_ago(1),
    schedule_interval = '@daily',
    tags = ['parameters', 'python']
) as dag:
    #instantiate the tasks of Python objects
    taskA = PythonOperator(
        task_id = 'greet_hello',
        python_callable = greet_hello,
        op_kwargs={'name': 'Desmond'}#op_kwargs allows to specify input parameters, can be a dict
    )

    taskB = PythonOperator(
        task_id = 'greet_hello_with_city',
        python_callable = greet_hello_with_city,
        op_kwargs={'name': 'Louise', 'city': 'Seattle'}
    )

#task A done before task B
taskA >> taskB



