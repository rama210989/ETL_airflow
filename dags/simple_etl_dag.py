from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

def print_hello():
    print("Hello, World!")

# Define the default_args dictionary (DO NOT include schedule_interval here)
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 5, 30),
}

# Create the DAG with schedule_interval as a separate argument
dag = DAG(
    'simple_etl_dag',
    default_args=default_args,  # Pass the default_args
    schedule_interval='@daily',  # Pass schedule_interval directly here
    catchup=False  # Optional: Avoid backfilling
)

# Create a task to print a message
task = PythonOperator(
    task_id='print_hello',
    python_callable=print_hello,
    dag=dag,
)

# Define task dependencies
task
