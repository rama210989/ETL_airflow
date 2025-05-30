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

# Create the DAG with schedule_interval outside default_args
dag = DAG(
    'simple_etl_dag',
    default_args=default_args,  # Pass default_args here
    schedule_interval='@daily',  # Define schedule_interval directly here
    catchup=False  # Avoid backfilling
)

# Create a task to print a message
task = PythonOperator(
    task_id='print_hello',
    python_callable=print_hello,
    dag=dag,
)

# Define task dependencies
task
