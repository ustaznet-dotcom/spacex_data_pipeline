FROM apache/airflow:2.9.1

USER airflow

RUN pip install --no-cache-dir \
    dbt-core==1.7.0 \
    dbt-postgres==1.7.0 \
    dbt-clickhouse==1.7.0