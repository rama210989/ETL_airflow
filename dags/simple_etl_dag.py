from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

def print_hello():
    print("Hello, World!")

# Define the default_args dictionary
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 5, 30),
}

# Correctly initializing the DAG for Airflow 3.x
with DAG(
    'simple_etl_dag',
    default_args=default_args,
    description='A simple ETL DAG',
    schedule_interval='@daily',  # Set the schedule interval
    catchup=False,
    max_active_runs=1
) as dag:
    # Create the task within the DAG context
    task = PythonOperator(
        task_id='print_hello',
        python_callable=print_hello
    )

    # You can define more tasks here and set dependencies
