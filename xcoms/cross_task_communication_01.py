import time

from datetime import datetime, timedelta
from airflow.utils.dates import days_ago

from airflow import DAG

from airflow.operators.python import PythonOperator

# XCom (cross-communication) allows tasks to exchange messages using Airflows cross-communication feature.

default_args = {
    'owner' : 'loonycorn'
}

# two functions that accepts a counter arg
def increment_by_1(counter):
    print("Count {counter}!".format(counter=counter))

    return counter + 1


def multiply_by_100(counter):
    print("Count {counter}!".format(counter=counter))

    return counter * 100


# def subtract_9(counter):
#     print("Count {counter}!".format(counter=counter))

#     return counter - 9

# def print(counter):
#     print("Count {counter}!".format(counter=counter))

#     return counter - 9

with DAG(
    dag_id = 'cross_task_communication',
    description = 'Cross-task communication with XCom',
    default_args = default_args,
    start_date = days_ago(1),
    schedule_interval = '@daily',
    tags = ['xcom', 'python']
) as dag:
    taskA = PythonOperator(
        task_id = 'increment_by_1',
        python_callable = increment_by_1,#call increment_by_1,
        op_kwargs={'counter': 100}#pass argument counter with value 100, retuns 101
    )

    taskB = PythonOperator(
        task_id = 'multiply_by_100',
        python_callable = multiply_by_100,
        op_kwargs={'counter': 9} #returns 900 because it multiplies 9 by 100 from the argument passed
    )

# task A will run first, then task B will run after task A is complete.
taskA >> taskB



