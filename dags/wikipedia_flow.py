
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.postgres_operator import PostgresOperator

import os
import sys
from datetime import datetime

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from pipelines.wikipedia_pipeline import transform_wikipedia_data,extract_wikipedia_data, write_wikipedia_data, load_to_database

default_args = {
    'owner': 'thang',
    'depends_on_past': False,
    'start_date': datetime(2024, 4, 20),
}

with DAG(
    dag_id='wikipedia_flow',
    default_args=default_args,
    schedule=None,
    catchup=False
) as dag:

    extract_data = PythonOperator(
        task_id="extract_wikipedia_data",
        python_callable=extract_wikipedia_data,
        provide_context=True,
        op_kwargs={"url": "https://en.wikipedia.org/wiki/List_of_association_football_stadiums_by_capacity"},
        dag=dag
    )

    transform_data = PythonOperator(
        task_id='transform_wikipedia_data',
        python_callable=transform_wikipedia_data,
        dag=dag
    )

    write_data = PythonOperator(
        task_id='write_wikipedia_data',
        provide_context=True,
        python_callable=write_wikipedia_data,
        dag=dag
    )

    load_data_to_database=PythonOperator(
        task_id='load_wikipedia_data',
        provide_context=True,
        python_callable=load_to_database,
        dag=dag

    )

    verify_data_in_database = PostgresOperator(
        task_id='verify_data_in_postgres',
        sql='SELECT COUNT(*) FROM word_stadium',
        postgres_conn_id='postgres-connection',
        dag=dag
    )
    extract_data >> transform_data >> write_data >> load_data_to_database >> verify_data_in_database