from airflow import DAG
from airflow.decorators import task
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertRowsOperator
from airflow.utils.dates import days_ago
import requests
import pandas as pd

# Constants
BQ_PROJECT = 'crypto-etl-project-461506'
BQ_DATASET = 'crypto_data'
BQ_TABLE = 'prices'

default_args = {
    'start_date': days_ago(1),
    'retries': 1,
}

with DAG(
    dag_id='crypto_prices_to_bigquery_v2',
    schedule_interval='*/15 * * * *',  # every 15 minutes
    default_args=default_args,
    catchup=False,
    max_active_runs=1,
    tags=['crypto', 'bigquery'],
) as dag:

    @task()
    def extract():
        """
        Extract latest crypto prices from CoinGecko API.
        """
        url = "https://api.coingecko.com/api/v3/simple/price?ids=bitcoin,ethereum&vs_currencies=usd"
        response = requests.get(url)
        response.raise_for_status()
        return response.json()

    @task()
    def transform(data):
        """
        Transform the raw JSON from CoinGecko into rows ready for BigQuery.
        """
        current_time = pd.Timestamp.utcnow()
        records = []

        for coin, price_info in data.items():
            record = {
                "coin": coin,
                "price_usd": float(price_info.get("usd", 0.0)),
                "timestamp": current_time.isoformat()
            }
            records.append(record)

        return records

    # Insert into BigQuery table
    load_to_bq = BigQueryInsertRowsOperator(
        task_id='load_to_bigquery',
        project_id=BQ_PROJECT,
        dataset_id=BQ_DATASET,
        table_id=BQ_TABLE,
        rows="{{ ti.xcom_pull(task_ids='transform') }}",
        gcp_conn_id='google_cloud_default',
    )

    # Define task dependencies
    raw_data = extract()
    transformed_data = transform(raw_data)
    load_to_bq << transformed_data
