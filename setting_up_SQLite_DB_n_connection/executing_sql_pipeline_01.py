from airflow import DAG

# import the operator to run SQLite commands on SQLite db
from airflow.operators.sqlite_operator import SqliteOperator

from datetime import date, datetime, timedelta
from airflow.utils.dates import days_ago

default_args = {
    'owner' : 'loonycorn'
}

#instantiate the DAG
with DAG(
    dag_id = 'executing_sql_pipeline',
    description = 'Pipeline using SQL operators',
    default_args = default_args,
    start_date = days_ago(1),
    schedule_interval = '@once',
    tags = ['pipeline', 'sql']
) as dag:
    # Instantiate the SQLite operator
    create_table = SqliteOperator(
        task_id = 'create_table',#id of the table
        #SQL statement -multiple line string
        sql = r"""
            CREATE TABLE IF NOT EXISTS users (
                    id INTEGER PRIMARY KEY,
                    name VARCHAR(50) NOT NULL,
                    age INTEGER NOT NULL,
                    is_active BOOLEAN DEFAULT true,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """,
        sqlite_conn_id = 'my_sqlite_conn',#sql type connection id, set up in airflow
        dag = dag,#create the dag
    )

create_table
