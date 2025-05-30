from airflow import DAG
from airflow.operators.python import PythonOperator
import pandas as pd
from datetime import datetime

# Define the extract, transform, and load functions
def extract_data():
    # Let's assume you're reading a CSV file. Replace with real data extraction logic
    data = pd.DataFrame({
        'name': ['Alice', 'Bob', 'Charlie'],
        'age': [25, 30, 35]
    })
    return data

def transform_data(data):
    # Adding a new column to the data (simple transformation)
    data['age_in_months'] = data['age'] * 12
    return data

def load_data(data):
    # Print the transformed data (you can replace this with DB or file write logic)
    print(data)
    return 'Data loaded successfully!'

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 5, 30),
}

# Define the DAG
dag = DAG(
    'simple_etl_dag',
    default_args=default_args,
    schedule_interval='@daily',  # Change to your desired schedule
)

# Define the tasks in the DAG
extract_task = PythonOperator(
    task_id='extract_data',
    python_callable=extract_data,
    dag=dag,
)

transform_task = PythonOperator(
    task_id='transform_data',
    python_callable=transform_data,
    op_args=['{{ task_instance.xcom_pull(task_ids="extract_data") }}'],  # Pulling data from extract task
    dag=dag,
)

load_task = PythonOperator(
    task_id='load_data',
    python_callable=load_data,
    op_args=['{{ task_instance.xcom_pull(task_ids="transform_data") }}'],  # Pulling transformed data
    dag=dag,
)

# Define task dependencies
extract_task >> transform_task >> load_task
