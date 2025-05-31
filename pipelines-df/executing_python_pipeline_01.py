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
    df = pd.read_csv('//home/katsa1k/airflow/dags/datasets/insurance.csv')

    print(df)

    return df.to_json()#returns as JSON format

#XCOM uses the serialized return from JSON format
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

read_csv_file