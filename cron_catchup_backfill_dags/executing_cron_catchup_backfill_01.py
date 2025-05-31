from datetime import datetime, timedelta
from airflow.utils.dates import days_ago

from airflow import DAG

from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator

# Use cron expression to schedule the DAG

from random import choice

default_args = {
   'owner' : 'loonycorn'
}

# return true or false random choice
def choose_branch():
    return choice([True, False])

# based on choose_branch, return and execute the taskC or taskD
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
    start_date = days_ago(5),
    #schedule_interval = '0 0 * * *',#Cron expression for scheduling in this DAG, Minute, Hour, Day of Month, Month, Year-i.e. cron should run midnight, * -everyday of month
    schedule_interval = '0 */12 * * 6,0',#run this task every 12 hours on on Sat and Sunday and repeat every weekend, 0 th day of week is Sun
    catchup = True #explicitly set to true, caught up with previous schedule dag
) as dag:

    taskA = BashOperator(
        task_id = 'taskA',
        bash_command = 'echo TASK A has executed!'
    )

    taskChoose = PythonOperator(
        task_id = 'taskChoose',# python operator for taskChoose
        python_callable = choose_branch
    )

    taskBranch = BranchPythonOperator(
        task_id = 'taskBranch', #python operator for task Branch
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

    taskE = EmptyOperator(#does nothing
        task_id = 'taskE',
    )


taskA >> taskChoose >> taskBranch >> [taskC, taskE]

taskC >> taskD


