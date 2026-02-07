import sys
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.operators.empty import EmptyOperator
from datetime import datetime

sys.path.append('/opt/airflow/src')
from load_to_greenplum import load_cities, load_conditions, load_forecast

dag1 = DAG(
    dag_id='extract_and_insert_cities',
    description='Extract and insert cities\' metadata to Greenplum',
    schedule='@once',
    start_date=datetime(2026, 2, 1),
    catchup=False,
)

with dag1:

    start = EmptyOperator(
        task_id='start_inserting_cities'
    )

    insert = PythonOperator(
        task_id='insert_cities',
        python_callable=load_cities
    )

    end = EmptyOperator(
        task_id='end_inserting_cities'
    )

    start >> insert >> end

dag2 = DAG(
    dag_id='extract_and_insert_conditions',
    description='Extract and insert conditions\' metadata to Greenplum',
    schedule='@once',
    start_date=datetime(2026, 2, 1),
    catchup=False,
)

with dag2:

    start = EmptyOperator(
        task_id='start_inserting_conditions'
    )

    insert = PythonOperator(
        task_id='insert_conditions',
        python_callable=load_conditions
    )

    end = EmptyOperator(
        task_id='end_inserting_conditions'
    )

    start >> insert >> end

dag3 = DAG(
    dag_id='extract_and_insert_forecast',
    description='Extract and insert forecast\' to Greenplum',
    schedule='@daily',
    start_date=datetime(2026, 2, 1),
    catchup=False,
)

with dag3:

    start = EmptyOperator(
        task_id='start_inserting_forecast'
    )

    insert = PythonOperator(
        task_id='insert_forecast',
        python_callable=load_forecast
    )

    end = EmptyOperator(
        task_id='end_inserting_forecast'
    )

    start >> insert >> end