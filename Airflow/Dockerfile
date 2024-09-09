FROM apache/airflow:2.7.3

# Install additional Python packages as airflow user
USER airflow

# Install Spark-related dependencies
RUN pip install apache-airflow[apache.spark]