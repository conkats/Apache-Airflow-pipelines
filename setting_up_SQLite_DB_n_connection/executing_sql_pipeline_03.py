from airflow import DAG

from airflow.operators.sqlite_operator import SqliteOperator

from datetime import date, datetime, timedelta
from airflow.utils.dates import days_ago

default_args = {
    'owner' : 'loonycorn'
}
# Instatiate the dag
with DAG(
    dag_id = 'executing_sql_pipeline',
    description = 'Pipeline using SQL operators',
    default_args = default_args,
    start_date = days_ago(1),
    schedule_interval = '@once',
    tags = ['pipeline', 'sql']
) as dag:
    #create the users table sqlite if it does not exist
    #city associate with each record
    create_table = SqliteOperator(
        task_id = 'create_table',
        sql = r"""
            CREATE TABLE IF NOT EXISTS users (
                    id INTEGER PRIMARY KEY,
                    name VARCHAR(50) NOT NULL,
                    age INTEGER NOT NULL,
                    city VARCHAR(50),
                    is_active BOOLEAN DEFAULT true,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """,
        sqlite_conn_id = 'my_sqlite_conn',
        dag = dag,
    )

    #insert some record to the .users table
    insert_values_1 = SqliteOperator(
        task_id = 'insert_values_1',
        sql = r"""
            INSERT INTO users (name, age, is_active) VALUES 
                ('Julie', 30, false),
                ('Peter', 55, true),
                ('Emily', 37, false),
                ('Katrina', 54, false),
                ('Joseph', 27, true);
        """,
        sqlite_conn_id = 'my_sqlite_conn',
        dag = dag,
    )

    insert_values_2 = SqliteOperator(
        task_id = 'insert_values_2',
        sql = r"""
            INSERT INTO users (name, age) VALUES 
                ('Harry', 49),
                ('Nancy', 52),
                ('Elvis', 26),
                ('Mia', 20);
        """,
        sqlite_conn_id = 'my_sqlite_conn',
        dag = dag,
    )#new operators to delete from users if user is inactive
    delete_values = SqliteOperator(
        task_id = 'delete_values',
        sql = r"""
            DELETE FROM users WHERE is_active = 0;
        """,
        sqlite_conn_id = 'my_sqlite_conn',
        dag = dag,
    )# new operator to update and set the city to Seattle for new users
    update_values = SqliteOperator(
        task_id = 'update_values',
        sql = r"""
            UPDATE users SET city = 'Seattle';
        """,
        sqlite_conn_id = 'my_sqlite_conn',
        dag = dag,
    )# display an dpush the result to xcom
    display_result = SqliteOperator(
        task_id = 'display_result',
        sql = r"""SELECT * FROM users""",
        sqlite_conn_id = 'my_sqlite_conn',
        dag = dag,
        do_xcom_push = True
    )

# operators and dependecies order
create_table >> [insert_values_1, insert_values_2] >> delete_values >> update_values >> display_result


