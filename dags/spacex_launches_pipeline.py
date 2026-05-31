from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
import requests
import json
from airflow.operators.bash import BashOperator

def fetch_from_api(endpoint, **context):
    """Fetch data from any SpaceX API v4 endpoint."""
    response = requests.get(f'https://api.spacexdata.com/v4/{endpoint}')
    response.raise_for_status()  
    return response.json()

def load_to_postgres(table_name, source_task_id, **context):
        ti = context['ti']
        records = ti.xcom_pull(task_ids=source_task_id)
        hook = PostgresHook(postgres_conn_id='spacex_postgres')
        sql = f"""
        INSERT INTO bronze.{table_name} (id, payload, dag_run_id)
        VALUES (%s, %s::jsonb, %s)
        ON CONFLICT (id) DO UPDATE
        SET payload = EXCLUDED.payload,
            loaded_at = CURRENT_TIMESTAMP,
            dag_run_id = EXCLUDED.dag_run_id;
"""
        for launch in records:
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

    create_bronze_tables = PostgresOperator(
        task_id='create_bronze_tables',
        postgres_conn_id='spacex_postgres',
        sql="""
            CREATE SCHEMA IF NOT EXISTS bronze;

            CREATE TABLE IF NOT EXISTS bronze.launches (
                id VARCHAR(50) PRIMARY KEY,
                payload JSONB NOT NULL,
                loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                dag_run_id VARCHAR(255)
            );

            CREATE TABLE IF NOT EXISTS bronze.rockets (
                id VARCHAR(50) PRIMARY KEY,
                payload JSONB NOT NULL,
                loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                dag_run_id VARCHAR(255)
            );

            CREATE TABLE IF NOT EXISTS bronze.launchpads (
                id VARCHAR(50) PRIMARY KEY,
                payload JSONB NOT NULL,
                loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                dag_run_id VARCHAR(255)
            );
        """,
     )
    fetch_launches = PythonOperator(
        task_id='fetch_launches',
        python_callable=fetch_from_api,
        op_kwargs={'endpoint': 'launches'}
    )

    load_launches = PythonOperator(
        task_id='load_launches',
        python_callable=load_to_postgres,
        op_kwargs={
        'table_name': 'launches',
        'source_task_id': 'fetch_launches',
        },
    )

    fetch_rockets = PythonOperator(
         task_id='fetch_rockets',
         python_callable=fetch_from_api,
         op_kwargs={'endpoint': 'rockets'},
    )

    load_rockets = PythonOperator(
    task_id='load_rockets',
    python_callable=load_to_postgres,
    op_kwargs={'table_name': 'rockets', 'source_task_id': 'fetch_rockets'},
)

    fetch_launchpads = PythonOperator(
    task_id='fetch_launchpads',
    python_callable=fetch_from_api,
    op_kwargs={'endpoint': 'launchpads'},
)
    load_launchpads = PythonOperator(
    task_id='load_launchpads',
    python_callable=load_to_postgres,
    op_kwargs={'table_name': 'launchpads', 'source_task_id': 'fetch_launchpads'},
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

    fetch_launches >> load_launches
    fetch_rockets >> load_rockets
    fetch_launchpads >> load_launchpads

    create_bronze_tables >> [fetch_launches, fetch_rockets, fetch_launchpads]
    [load_launches, load_rockets, load_launchpads] >> dbt_run_silver >> dbt_test >> dbt_run_gold
