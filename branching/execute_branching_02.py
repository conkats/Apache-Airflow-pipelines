import pandas as pd

from datetime import datetime, timedelta
from airflow.utils.dates import days_ago

from airflow import DAG
#import python branch operator
from airflow.operators.python import PythonOperator, BranchPythonOperator
#import the variable clase
from airflow.models import Variable


default_args = {
   'owner': 'loonycorn'
}

#define the paths input and output of transform
DATASETS_PATH = '/home/katsa1k/airflow/dags/datasets/insurance.csv'

OUTPUT_PATH = '/home/katsa1k/airflow/dags/output/{0}.csv'

#read the file and convert to json format
def read_csv_file():
    df = pd.read_csv(DATASETS_PATH)

    print(df)

    return df.to_json()


#remove the null values from the df
def remove_null_values(ti):
    json_data = ti.xcom_pull(task_ids='read_csv_file')
    
    df = pd.read_json(json_data)
    
    df = df.dropna()

    print(df)

    return df.to_json()

# branching function, using the variable transform_action
#transform_action starts with the term filter,
# it's a filtering action and the transform_action string itself specifies the task ID to execute. 
def determine_branch():
    transform_action = Variable.get("transform_action", default_var=None)
    
    #if the action starts
    if transform_action.startswith('filter'):
        return transform_action
    elif transform_action == 'groupby_region_smoker':
        return 'groupby_region_smoker'


#all functions to be performed
def filter_by_southwest(ti):
    #access the json data, convert to df and filter by region
    json_data = ti.xcom_pull(task_ids='remove_null_values')
    df = pd.read_json(json_data)

    region_df = df[df['region'] == 'southwest']
    
    #output the result in the outputpath
    region_df.to_csv(OUTPUT_PATH.format('southwest'), index=False)


def filter_by_southeast(ti):
    json_data = ti.xcom_pull(task_ids='remove_null_values')
    df = pd.read_json(json_data)

    region_df = df[df['region'] == 'southeast']
    
    region_df.to_csv(OUTPUT_PATH.format('southeast'), index=False)


def filter_by_northwest(ti):
    json_data = ti.xcom_pull(task_ids='remove_null_values')
    df = pd.read_json(json_data)

    region_df = df[df['region'] == 'northwest']
    
    region_df.to_csv(OUTPUT_PATH.format('northwest'), index=False)


def filter_by_northeast(ti):
    json_data = ti.xcom_pull(task_ids='remove_null_values')
    df = pd.read_json(json_data)

    region_df = df[df['region'] == 'northeast']
    
    region_df.to_csv(OUTPUT_PATH.format('northeast'), index=False)


# grouping task
def groupby_region_smoker(ti):
    json_data = ti.xcom_pull(task_ids='remove_null_values')
    df = pd.read_json(json_data)

    region_df = df.groupby('region').agg({
        'age': 'mean', 
        'bmi': 'mean',
        'charges': 'mean'
    }).reset_index()
    #writeout to .csv
    region_df.to_csv(OUTPUT_PATH.format('grouped_by_region'), index=False)

    smoker_df = df.groupby('smoker').agg({
        'age': 'mean', 
        'bmi': 'mean',
        'charges': 'mean'
    }).reset_index()

    region_df.to_csv(OUTPUT_PATH.format('grouped_by_smoker'), index=False)


# instatiate the DAG    
with DAG(
    dag_id = 'executing_branching',
    description = 'Running a branching pipeline',
    default_args = default_args,
    start_date = days_ago(1),
    schedule_interval = '@once',
    tags = ['python', 'transform', 'pipeline', 'branching']
) as dag:
    #the first 3 tasks
    read_csv_file = PythonOperator(
        task_id='read_csv_file',
        python_callable=read_csv_file
    )

    remove_null_values = PythonOperator(
        task_id='remove_null_values',
        python_callable=remove_null_values
    )

    determine_branch = BranchPythonOperator(
        task_id='determine_branch',
        python_callable=determine_branch
    )
    #calls to filter tasks
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
    # Python operator for group definition
    groupby_region_smoker = PythonOperator(
        task_id='groupby_region_smoker',
        python_callable=groupby_region_smoker
    )
    #DAG definition, tasks, determine the branch and then post operations
    read_csv_file >> remove_null_values >> determine_branch >> [filter_by_southwest, 
                                                                filter_by_southeast, 
                                                                filter_by_northwest,
                                                                filter_by_northeast,
                                                                groupby_region_smoker]

