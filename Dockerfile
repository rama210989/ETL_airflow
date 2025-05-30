# Use Astronomer's official Airflow image as the base image
FROM astronomerinc/ap-airflow:2.1.2

# Set environment variables (timezone, Airflow home directory)
ENV AIRFLOW_HOME=/usr/local/airflow
ENV AIRFLOW__CORE__LOAD_EXAMPLES=False

# Install dependencies (requests, beautifulsoup, etc.)
RUN pip install --no-cache-dir \
    requests \
    beautifulsoup4 \
    pandas \
    psycopg2-binary \
    apache-airflow-providers-http

# Set the working directory
WORKDIR /usr/local/airflow

# Copy the DAGs and Python files into the container
COPY ./airflow/dags /usr/local/airflow/dags

# Expose the necessary ports for Airflow UI and Webserver
EXPOSE 8080
