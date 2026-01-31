FROM apache/airflow:2.9.3-python3.10

USER root

RUN pip install --no-cache-dir apache-airflow-providers-databricks

USER airflow
