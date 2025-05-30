from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

def print_hello():
    print("Hello, World!")

# Define the default_args dictionary
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 5, 30),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Use DAG context manager to define your DAG
with DAG(
    dag_id='simple_etl_dag',
    default_args=default_args,
    description='A simple ETL DAG',
    schedule_interval='@daily',  # This is where the schedule is set
    catchup=False,
    max_active_runs=1
) as dag:

    # Create the task within the DAG context
    task = PythonOperator(
        task_id='print_hello',
        python_callable=print_hello
    )
