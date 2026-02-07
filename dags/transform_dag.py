import sys
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.operators.empty import EmptyOperator
from datetime import datetime

sys.path.append('/opt/airflow/src')
from transform import load_to_olap

dag = DAG(
    dag_id='transform_and_load_to_olap',
    description='Transform and load data to OLAP DB Clickhouse',
    schedule='@daily',
    start_date=datetime(2026, 2, 1),
    catchup=False,
)

with dag:

    start = EmptyOperator(
        task_id='start_transforming_data'
    )

    insert = PythonOperator(
        task_id='transform_data',
        python_callable=load_to_olap
    )

    end = EmptyOperator(
        task_id='end_transforming_data'
    )

    start >> insert >> end