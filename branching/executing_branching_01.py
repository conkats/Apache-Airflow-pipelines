# Simple branching
# implementing conditional branching
from datetime import datetime, timedelta
from airflow.utils.dates import days_ago

from airflow import DAG
# import the branch python operator from airflow
from airflow.operators.python import PythonOperator, BranchPythonOperator

from random import choice

default_args = {
    'owner' : 'loonycorn',
}

# method to return a random choice of true of false for driving license
def has_driving_license():
    return choice([True, False])

# function for branch operator, takes in the task instance has_driving_license
# check if the person has one
def branch(ti):
    if ti.xcom_pull(task_ids='has_driving_license'):#if true
        return 'eligible_to_drive'#call the functions below
    else:
        return 'not_eligible_to_drive'                      

#based on the above branching conditions, the functions wil be executed
def eligible_to_drive():
    print("You can drive, you have a license!")

def not_eligible_to_drive():
    print("I'm afraid you are out of luck, you need a license to drive")


with DAG(
    dag_id = 'executing_branching',
    description = 'Running branching pipelines',
    default_args = default_args,
    start_date = days_ago(1),
    schedule_interval = '@once',
    tags = ['branching', 'conditions']
) as dag:
    # 4 python operators, id, conditional branch op, function for eligible to drive, or not
    taskA = PythonOperator(
        task_id = 'has_driving_license',
        python_callable = has_driving_license              
    )

    taskB = BranchPythonOperator(
        task_id = 'branch',
        python_callable = branch
    )

    taskC = PythonOperator(
        task_id = 'eligible_to_drive',
        python_callable = eligible_to_drive              
    )

    taskD = PythonOperator(
        task_id = 'not_eligible_to_drive',
        python_callable = not_eligible_to_drive              
    )

#dependencies based on branch
taskA >> taskB >> [taskC, taskD]