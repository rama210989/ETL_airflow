# Use Astronomer's official Airflow 3.x image as the base image
FROM astronomerinc/ap-airflow:3.0.1

# Set environment variables (timezone, Airflow home directory)
ENV AIRFLOW_HOME=/usr/local/airflow
ENV AIRFLOW__CORE__LOAD_EXAMPLES=False

# Install necessary dependencies (pandas, psycopg2-binary for PostgreSQL)
RUN pip install --no-cache-dir \
    pandas \
    psycopg2-binary

# Set the working directory
WORKDIR /usr/local/airflow

# Copy the DAGs and Python files into the container
COPY ./dags /usr/local/airflow/dags

# Expose the necessary ports for Airflow UI and Webserver
EXPOSE 8080
