import pandas as pd

from datetime import datetime, timedelta
from airflow.utils.dates import days_ago

from airflow import DAG
from airflow.operators.python_operator import PythonOperator

### Pipeline to read a CSV file and print its contents
default_args = {
   'owner': 'loonycorn'
}

#read the csv from local filesystem
def read_csv_file():
    df = pd.read_csv('/home/katsa1k/airflow/dags/datasets/insurance.csv')

    print(df)

    return df.to_json()#returns as JSON format

#XCOM uses the serialized return from JSON format
# variable arguments using kwargs
def remove_null_values(**kwargs):
    # accessing the task instance by looking up ti and kwards
    ti = kwargs['ti']
    # pulling the JSONdata from the previous task using xcom_pull
    json_data = ti.xcom_pull(task_ids='read_csv_file')
    
    # get it back to a DataFrame from JSON
    df = pd.read_json(json_data)
    #clean the df
    df = df.dropna()

    print(df)
    # return the cleaned DataFrame as JSON formatt
    return df.to_json()


with DAG(
    'python_pipeline',
    description='Running a Python pipeline',
    default_args=default_args,
    start_date=days_ago(1),
    schedule_interval='@once',
    tags = ['python', 'transform', 'pipeline']
) as dag:
    
    read_csv_file = PythonOperator(
        task_id='read_csv_file',#invoved the python callable to read csv
        python_callable=read_csv_file
    )

    remove_null_values = PythonOperator(
        task_id='remove_null_values',#invoved the python callable to read csv
        python_callable=remove_null_values
    )
    
    # two tasks, read_csv_file comes first, then remove_null_values
    read_csv_file >> remove_null_values




