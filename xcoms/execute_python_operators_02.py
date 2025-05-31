import time

from datetime import datetime, timedelta
from airflow.utils.dates import days_ago

from airflow import DAG

from airflow.operators.python import PythonOperator

default_args = {
    'owner' : 'loonycorn'
}

#functions defined for python print statemets to be invoked by PythonOperator
def task_a():
    print("TASK A executed!")

def task_b():
    time.sleep(5)
    print("TASK B executed!")

def task_c():
    print("TASK C executed!")

def task_d():
    print("TASK D executed!")            

with DAG(
    dag_id = 'execute_python_operators',
    description = 'Python operators in DAGs',
    default_args = default_args,
    start_date = days_ago(1),
    schedule_interval = '@daily',
    tags = ['dependencies', 'python']
) as dag:
    #each task is defined using PythonOperator
    #taskA is the first task, which is executed first
    #etc.
    taskA = PythonOperator(
        task_id = 'taskA',
        python_callable = task_a
    )

    taskB = PythonOperator(
        task_id = 'taskB',
        python_callable = task_b
    )

    taskC = PythonOperator(
        task_id = 'taskC',
        python_callable = task_c
    )

    taskD = PythonOperator(
        task_id = 'taskD',
        python_callable = task_d
    )

taskA >> [taskB, taskC]#taskA is executed first, then taskB and taskC in parallel
[taskB, taskC] >> taskD



