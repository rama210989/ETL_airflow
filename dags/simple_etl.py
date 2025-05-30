from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import pandas as pd
import logging

def extract():
    logging.info("Extracting data...")
    data = {
        "name": ["Alice", "Bob", "Charlie"],
        "age": [25, 30, None]
    }
    df = pd.DataFrame(data)
    df.to_csv("/tmp/extracted.csv", index=False)

def transform():
    logging.info("Transforming data...")
    df = pd.read_csv("/tmp/extracted.csv")
    df['age'] = df['age'].fillna(df['age'].mean())
    df.to_csv("/tmp/transformed.csv", index=False)

def load():
    logging.info("Loading data...")
    df = pd.read_csv("/tmp/transformed.csv")
    df.to_csv("/tmp/final_output.csv", index=False)

with DAG(
    dag_id="simple_etl_demo",
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
    description="A simple ETL demo DAG"
) as dag:
    
    t1 = PythonOperator(task_id="extract", python_callable=extract)
    t2 = PythonOperator(task_id="transform", python_callable=transform)
    t3 = PythonOperator(task_id="load", python_callable=load)

    t1 >> t2 >> t3
