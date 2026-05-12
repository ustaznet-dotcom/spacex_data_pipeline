from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
import requests
import json
from airflow.operators.bash import BashOperator

def fetch_launches_from_api(**context):
        response = requests.get('https://api.spacexdata.com/v4/launches')

        return response.json()

def load_to_postgres(**context):
        ti = context['ti']

        launches  = ti.xcom_pull(task_ids='fetch_launches_from_api')
        hook = PostgresHook(postgres_conn_id='spacex_postgres')
        sql = """
        INSERT INTO bronze.launches (id, payload, dag_run_id)
        VALUES (%s, %s::jsonb, %s)
        ON CONFLICT (id) DO UPDATE
        SET payload = EXCLUDED.payload,
            loaded_at = CURRENT_TIMESTAMP,
            dag_run_id = EXCLUDED.dag_run_id;
"""
        for launch in launches:
            hook.run(sql, parameters=(
                    launch['id'],
                    json.dumps(launch),
                    context['dag_run'].run_id,
                ))


with DAG(
    dag_id='spacex_launches_pipeline',
    tags=['spacex', 'bronze', 'silver', 'dbt', 'clickhouse', 'gold'],
    start_date=datetime(2026, 1, 1),
    schedule='0 6 * * *',
    catchup=False,
) as dag:

    create_bronze_table = PostgresOperator(
        task_id='create_bronze_table',
        postgres_conn_id='spacex_postgres',
        sql=""" CREATE SCHEMA IF NOT EXISTS bronze;
          CREATE TABLE IF NOT EXISTS bronze.launches (
        id VARCHAR(50) PRIMARY KEY,
        payload JSONB NOT NULL,
        loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        dag_run_id VARCHAR(255));
""",
    )


    fetch_launches = PythonOperator(
        task_id='fetch_launches_from_api',
        python_callable=fetch_launches_from_api,
    )

    load_data = PythonOperator(
        task_id='load_to_postgres',
        python_callable=load_to_postgres,
    )
    dbt_run_silver = BashOperator(
          task_id='dbt_run_silver',
          bash_command='cd /opt/airflow/dbt/spacex_dbt && dbt run --select silver.* --target dev --profiles-dir .',

    )

    dbt_test=BashOperator(
          task_id='dbt_test',
          bash_command='cd /opt/airflow/dbt/spacex_dbt && dbt test --profiles-dir .',
    )

    dbt_run_gold=BashOperator(
          task_id='dbt_run_gold',
            bash_command='cd /opt/airflow/dbt/spacex_dbt && dbt run --select gold.* --target clickhouse --profiles-dir .',
    )

    create_bronze_table >> fetch_launches >> load_data >> dbt_run_silver >> dbt_test >> dbt_run_gold
