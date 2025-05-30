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

# Use the `with` statement to define the DAG, this ensures backward compatibility
with DAG(
    dag_id='simple_etl_dag',
    default_args=default_args,
    schedule_interval='@daily',  # Schedule interval directly in the context
    catchup=False,
    max_active_runs=1
) as dag:
    
    # Create the task within the DAG context
    task = PythonOperator(
        task_id='print_hello',
        python_callable=print_hello,
    )

    # Define task dependencies (if more tasks are added later)
    task
