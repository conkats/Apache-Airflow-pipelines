from datetime import datetime, timedelta
from airflow.utils.dates import days_ago

from airflow import DAG

from airflow.operators.bash import BashOperator

default_args = {
   'owner' : 'loonycorn'
}

#with DAG(
#    dag_id = 'executing_multiple_tasks',
#    description = 'DAG with multiple tasks and dependencies',
#    default_args = default_args,
#    start_date = days_ago(1),
#    #schedule_interval = '@once' # external trigger for this DAG
#    schedule_interval = 'timedelta(days=1)', # external trigger for this DAG on daily basis
#    tags = ['upstream', 'downstream']
#) as dag:
    
#with DAG(
#    dag_id = 'executing_multiple_tasks',
#    description = 'DAG with multiple tasks and dependencies',
#    default_args = default_args,
#    start_date = days_ago(1),
#    schedule_interval = timedelta(days=1),
#    tags = ['upstream', 'downstream']
#) as dag:

with DAG(
    dag_id = 'executing_multiple_tasks',
    description = 'DAG with multiple tasks and dependencies',
    default_args = default_args,
    start_date = days_ago(1),
    schedule_interval = timedelta(days=1),
    tags = ['scripts', 'template_search'],
    template_searchpath = '/home/katsa1k/airflow/dags/bash_scripts'

) as dag:
    
#    # two operators-tasks A and B
#    taskA = BashOperator(
#        task_id = 'taskA',
#        #bash_command = 'echo TASK A has executed!'
#        bash_command = '''
#            echo TASK A has executed!
#
#            for i in {1..10}
#            do 
#                echo TASK A is printing $i
#            done
#            echo TASK A has ended!         
#       '''
#    )
#
#    taskB = BashOperator(
#        task_id = 'taskB',
#        #bash_command = 'echo TASK B has executed!'
#        bash_command = '''
#            echo TASK B has started!
#            sleep 4
#            echo TASK B has ended!
#        '''
#
#    )
#
#    taskC = BashOperator(
#        task_id = 'taskC',
#        bash_command = '''
#            echo TASK C has started!
#            sleep 15
#            echo TASK C has ended!
#        '''
#    )
#    
#    taskD = BashOperator(
#        task_id = 'taskD',
#        bash_command = 'echo TASK D completed!'
#    )
#
## A depends on B and C
##taskA.set_downstream(taskB)
##taskA >> taskB
##taskA.set_downstream(taskC)
##taskA >> taskC
#taskA >> [taskB, taskC]
#
## D depends on B and C
##taskD.set_upstream(taskB)
##taskD << taskB
##taskD.set_upstream(taskC)
##taskD << taskC
#
#
#taskD << [taskB, taskC]
## set_downstream() method 
## taskA.set_downstream(taskB)
##taskA.set_downstream(taskB)
##taskA.set_upstream(taskB)
    taskA = BashOperator(
        task_id = 'taskA',
        bash_command = 'taskA.sh' # point to the .sh file
    )

    taskB = BashOperator(
        task_id = 'taskB',
        bash_command = 'taskB.sh'
    )

    taskC = BashOperator(
        task_id = 'taskC',
        bash_command = 'taskC.sh'
    )
    
    taskD = BashOperator(
        task_id = 'taskD',
        bash_command = 'taskD.sh'
    )

    taskE = BashOperator(
        task_id = 'taskE',
        bash_command = 'taskE.sh'
    )

    taskF = BashOperator(
        task_id = 'taskF',
        bash_command = 'taskF.sh'
    )

    taskG = BashOperator(
        task_id = 'taskG',
        bash_command = 'taskG.sh'
    )

taskA >> taskB >> taskE

taskA >> taskC >> taskF

taskA >> taskD >> taskG