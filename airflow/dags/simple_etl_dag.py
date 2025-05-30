from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import requests
from bs4 import BeautifulSoup
import csv
import os

# Function to scrape weather data from a weather website
def extract_data():
    url = "https://weather.com/weather/today/l/37.77,-122.42"  # San Francisco weather
    response = requests.get(url)
    soup = BeautifulSoup(response.text, 'html.parser')
    
    # Extracting data (let's say we scrape the temperature)
    temperature = soup.find('span', {'class': 'CurrentConditions--tempValue--3KcTQ'}).get_text()
    
    return temperature

# Function to transform the scraped data (here we will just format it)
def transform_data(data):
    cleaned_data = f"Temperature: {data}°F"
    
    # Save to CSV
    if not os.path.exists('data.csv'):
        with open('data.csv', mode='w', newline='') as file:
            writer = csv.writer(file)
            writer.writerow(["Weather Data"])
            writer.writerow([cleaned_data])
    
    return 'data.csv'

# Function to load the data (for now, we'll just print it or save as a CSV)
def load_data(csv_file):
    # Just a placeholder, since we're writing directly to CSV
    print(f"Data saved to: {csv_file}")

# Define default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 5, 30),  # Change as needed
    'retries': 1,
}

# Define the DAG
dag = DAG(
    'simple_etl_dag',
    default_args=default_args,
    description='Simple ETL with Weather Data',
    schedule_interval='@daily',
)

# Define tasks
extract_task = PythonOperator(
    task_id='extract_data',
    python_callable=extract_data,
    dag=dag,
)

transform_task = PythonOperator(
    task_id='transform_data',
    python_callable=transform_data,
    op_args=['{{ task_instance.xcom_pull(task_ids="extract_data") }}'],  # Passing data from extract
    dag=dag,
)

load_task = PythonOperator(
    task_id='load_data',
    python_callable=load_data,
    op_args=['{{ task_instance.xcom_pull(task_ids="transform_data") }}'],  # Passing data from transform
    dag=dag,
)

# Set task dependencies
extract_task >> transform_task >> load_task
