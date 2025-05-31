from airflow import DAG
from airflow.decorators import task
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from datetime import datetime, timedelta
import requests
import pandas as pd


BQ_PROJECT = 'crypto-etl-project-461506'
BQ_DATASET = 'crypto_data'
BQ_TABLE = 'prices'

default_args = {
    'start_date': datetime.utcnow() - timedelta(days=1),  # ✅
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
        url = "https://api.coingecko.com/api/v3/simple/price?ids=bitcoin,ethereum&vs_currencies=usd"
        response = requests.get(url)
        response.raise_for_status()
        return response.json()

    @task()
    def transform(data):
        current_time = pd.Timestamp.utcnow()
        rows = []
        for coin, price_info in data.items():
            row = {
                "coin": coin,
                "price_usd": float(price_info.get("usd", 0.0)),
                "timestamp": current_time.isoformat()
            }
            rows.append(row)
        return rows

    @task()
    def build_sql(rows):
        values = ",\n".join([
            f"('{r['coin']}', {r['price_usd']}, TIMESTAMP('{r['timestamp']}'))"
            for r in rows
        ])
        return f"""
        INSERT INTO `{BQ_PROJECT}.{BQ_DATASET}.{BQ_TABLE}` (coin, price_usd, timestamp)
        VALUES
        {values}
        """

    load = BigQueryInsertJobOperator(
        task_id='load_to_bigquery',
        configuration={
            "query": {
                "query": "{{ ti.xcom_pull(task_ids='build_sql') }}",
                "useLegacySql": False,
            }
        },
        location="US",
        gcp_conn_id="google_cloud_default",
    )

    # DAG structure
    extracted = extract()
    transformed = transform(extracted)
    sql = build_sql(transformed)
    load << sql
