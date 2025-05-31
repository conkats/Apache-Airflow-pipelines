import pandas as pd

from datetime import datetime, timedelta
from airflow.utils.dates import days_ago

from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.models import Variable
from airflow.utils.task_group import TaskGroup
from airflow.utils.edgemodifier import Label


default_args = {
   'owner': 'loonycorn'
}

DATASETS_PATH = '/home/katsa1k/airflow/dags/datasets/insurance.csv'

OUTPUT_PATH = '/home/katsa1k/airflow/dags/output/{0}.csv'

def read_csv_file(ti):
    df = pd.read_csv(DATASETS_PATH)

    print(df)
    # push the value to xcom json value that I want to pass on the other task to the key in my_csv
    ti.xcom_push(key='my_csv', value=df.to_json())

#task id is the input arguement to remove the values from dataframe
def remove_null_values(ti):
    json_data = ti.xcom_pull(key='my_csv')
    
    df = pd.read_json(json_data)
    
    df = df.dropna()

    print(df)

    ti.xcom_push(key='my_clean_csv', value=df.to_json())

# read the transform_action variable
def determine_branch():
    transform_action = Variable.get("transform_action", default_var=None)
    
    print(transform_action)

    if transform_action.startswith('filter'):
        # filter by filter id from the task group, by name pre-fix
        return "filtering.{0}".format(transform_action)
    elif transform_action == 'groupby_region_smoker':
        return "grouping.{0}".format(transform_action)


def filter_by_southwest(ti):
    json_data = ti.xcom_pull(key='my_clean_csv')
    df = pd.read_json(json_data)

    region_df = df[df['region'] == 'southwest']
    
    region_df.to_csv(OUTPUT_PATH.format('southwest'), index=False)


def filter_by_southeast(ti):
    json_data = ti.xcom_pull(key='my_clean_csv')
    df = pd.read_json(json_data)

    region_df = df[df['region'] == 'southeast']
    
    region_df.to_csv(OUTPUT_PATH.format('southeast'), index=False)


def filter_by_northwest(ti):
    json_data = ti.xcom_pull(key='my_clean_csv')
    df = pd.read_json(json_data)

    region_df = df[df['region'] == 'northwest']
    
    region_df.to_csv(OUTPUT_PATH.format('northwest'), index=False)


def filter_by_northeast(ti):
    json_data = ti.xcom_pull(key='my_clean_csv')
    df = pd.read_json(json_data)

    region_df = df[df['region'] == 'northeast']
    
    region_df.to_csv(OUTPUT_PATH.format('northeast'), index=False)


def groupby_region_smoker(ti):
    json_data = ti.xcom_pull(key='my_clean_csv')
    df = pd.read_json(json_data)

    region_df = df.groupby('region').agg({
        'age': 'mean', 
        'bmi': 'mean',
        'charges': 'mean'
    }).reset_index()

    region_df.to_csv(OUTPUT_PATH.format('grouped_by_region'), index=False)

    smoker_df = df.groupby('smoker').agg({
        'age': 'mean', 
        'bmi': 'mean',
        'charges': 'mean'
    }).reset_index()

    smoker_df.to_csv(OUTPUT_PATH.format('grouped_by_smoker'), index=False)


    
with DAG(
    dag_id = 'taskgroups_and_edgelabels',
    description = 'Running a branching pipeline with TaskGroups and EdgeLabels',
    default_args = default_args,
    start_date = days_ago(1),
    schedule_interval = '@once',
    tags = ['python', 'taskgroups', 'edgelabels', 'branching']
) as dag:
    #reading and pre-processing
    with TaskGroup('reading_and_preprocessing') as reading_and_preprocessing:
        # remove null values
        read_csv_file = PythonOperator(
            task_id='read_csv_file',
            python_callable=read_csv_file
        )

        remove_null_values = PythonOperator(
            task_id='remove_null_values',
            python_callable=remove_null_values
        )
        # csv comes first and then remove null values
        read_csv_file >> remove_null_values

    determine_branch = BranchPythonOperator(
        task_id='determine_branch',
        python_callable=determine_branch
    )
    # filter tasks
    # or are indepent
    with TaskGroup('filtering') as filtering:
        filter_by_southwest = PythonOperator(
            task_id='filter_by_southwest',
            python_callable=filter_by_southwest
        )
        
        filter_by_southeast = PythonOperator(
            task_id='filter_by_southeast',
            python_callable=filter_by_southeast
        )

        filter_by_northwest = PythonOperator(
            task_id='filter_by_northwest',
            python_callable=filter_by_northwest
        )

        filter_by_northeast = PythonOperator(
            task_id='filter_by_northeast',
            python_callable=filter_by_northeast
        )

    #another task for grouping
    with TaskGroup('grouping') as grouping:
        groupby_region_smoker = PythonOperator(
            task_id='groupby_region_smoker',
            python_callable=groupby_region_smoker
        )
    #specify the taks, determine the branch
    reading_and_preprocessing >> Label('preprocessed data') >> determine_branch >> Label('branch on condition') >> [filtering, grouping]
