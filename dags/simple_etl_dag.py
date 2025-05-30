from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

def print_hello():
    print("Hello, World!")

# Define the default_args dictionary (DO NOT include schedule_interval here)
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 5, 30),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Create the DAG with proper arguments
dag = DAG(
    dag_id='simple_etl_dag',
    default_args=default_args,  # Pass the default_args
    schedule_interval='@daily',  # Schedule the DAG
    catchup=False,  # Avoid backfilling
    max_active_runs=1  # Limit the number of active DAG runs
)

# Create a task to print a message
task = PythonOperator(
    task_id='print_hello',
    python_callable=print_hello,
    dag=dag,
)

# Define task dependencies (if more tasks exist in the future)
task
