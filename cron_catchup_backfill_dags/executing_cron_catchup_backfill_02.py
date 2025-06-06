# Backfill
#Airflow. Now, backfill refers specifically to the process of
#  executing past DAG runs for a specific gate range rather than all past DAG runs. Backfill can be useful
#  when you need to rerun a subset of the 
# past DAG runs, maybe the runs for a particular week or a month. 
#run back fill
#conda activate 
#airflow dags backfill -s 2022-04-01 -e 2022-04-20 cron_catchup_backfil
from datetime import datetime, timedelta
from airflow.utils.dates import days_ago

from airflow import DAG

from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator

from random import choice

default_args = {
   'owner' : 'loonycorn'
}

def choose_branch():
    return choice([True, False])

def branch(ti):
    if ti.xcom_pull(task_ids='taskChoose'):
        return 'taskC'
    else:
        return 'taskD'   

def task_c():
    print("TASK C executed!")


with DAG(
    dag_id = 'cron_catchup_backfill',
    description = 'Using crons, catchup, and backfill',
    default_args = default_args,
    start_date = days_ago(30),#start for the DAG is 30 days ago
    schedule_interval = '0 */12 * * 6 0',#every Sat, Sun every 12 hours-4 runs everyweekend
    catchup = False#DAG will run exactly once instead of previous runs
) as dag:

    taskA = BashOperator(
        task_id = 'taskA',
        bash_command = 'echo TASK A has executed!'
    )

    taskChoose = PythonOperator(
        task_id = 'taskChoose',
        python_callable = choose_branch
    )

    taskBranch = BranchPythonOperator(
        task_id = 'taskBranch',
        python_callable = branch
    )

    taskC = PythonOperator(
        task_id = 'taskC',
        python_callable = task_c
    )

    taskD = BashOperator(
        task_id = 'taskD',
        bash_command = 'echo TASK D has executed!'
    )

    taskE = EmptyOperator(
        task_id = 'taskE',
    )


taskA >> taskChoose >> taskBranch >> [taskC, taskE]

taskC >> taskD


