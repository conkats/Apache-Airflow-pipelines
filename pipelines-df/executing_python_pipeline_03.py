import pandas as pd
import matplotlib.pyplot as plt

from datetime import datetime, timedelta
from airflow.utils.dates import days_ago

from airflow import DAG
from airflow.operators.python_operator import PythonOperator


default_args = {
   'owner': 'loonycorn'
}


def read_csv_file():
    df = pd.read_csv('/home/katsa1k/airflow/dags/datasets/insurance.csv')

    print(df)

    return df.to_json()

# function to remove null values from the DataFrame
# and return the cleaned DataFrame as JSON
def remove_null_values(ti):
    json_data = ti.xcom_pull(task_ids='read_csv_file')
    
    df = pd.read_json(json_data)
    
    df = df.dropna()

    print(df)

    return df.to_json()

# function to group the DataFrame by 'smoker' 
# takes task instance as an argument and call xcom_pull to get the cleaned DataFrame
def groupby_smoker(ti):
    json_data = ti.xcom_pull(task_ids='remove_null_values')
    df = pd.read_json(json_data)#convert JSON back to DataFrame
    
    # apply transformation to group by 'smoker' and 
    # calculate mean for 'age', 'bmi', and 'charges'
    smoker_df = df.groupby('smoker').agg({
        'age': 'mean', 
        'bmi': 'mean',
        'charges': 'mean'
    }).reset_index()
    # save the grouped DataFrame to a CSV file
    smoker_df.to_csv(
        '/home/katsa1k/airflow/dags/output/grouped_by_smoker.csv', index=False)

# takes task instance as an argument and call xcom_pull to get the cleaned DataFrame
# multiple tasks to access the data from XCOM
def groupby_region(ti):
    json_data = ti.xcom_pull(task_ids='remove_null_values')
    df = pd.read_json(json_data)
    # apply transformation to group by 'region' and
    # calculate mean for 'age', 'bmi', and 'charges'
    region_df = df.groupby('region').agg({
        'age': 'mean', 
        'bmi': 'mean', 
        'charges': 'mean'
    }).reset_index()
    

    region_df.to_csv(
        '/home/katsa1k/airflow/dags/output/grouped_by_region.csv', index=False)

# define the DAG with the default arguments and schedule
with DAG(
    dag_id = 'python_pipeline',
    description = 'Running a Python pipeline',
    default_args = default_args,
    start_date = days_ago(1),
    schedule_interval = '@once',
    tags = ['python', 'transform', 'pipeline']
) as dag:
    
    read_csv_file = PythonOperator(
        task_id='read_csv_file',
        python_callable=read_csv_file
    )

    remove_null_values = PythonOperator(
        task_id='remove_null_values',
        python_callable=remove_null_values
    )
    
    groupby_smoker = PythonOperator(
        task_id='groupby_smoker',
        python_callable=groupby_smoker
    )
    
    groupby_region = PythonOperator(
        task_id='groupby_region',
        python_callable=groupby_region
    )

read_csv_file >> remove_null_values >> [groupby_smoker, groupby_region]


