from datetime import datetime

from airflow import DAG
from airflow.providers.google.cloud.operators.gcs import GCSListObjectsOperator
from airflow.providers.google.cloud.operators.bigquery import  BigQueryInsertJobOperator
from airflow.operators.python import PythonOperator
import os
import re

BUCKET = os.getenv('BUCKET_NAME')

DATASET_NAME  = os.getenv('BQ_DATASET_NAME')


def gcs_objects_to_load(**kwargs):
    ti = kwargs["ti"]
    objects_list = ti.xcom_pull(task_ids='get_all_objects')
    current_date = kwargs['ds'].replace('-', '')                                #The ds represents the execution date so the dag can be
    pattern = re.compile(rf'preprocessed/.*/{current_date}-\d+\.csv$')
    object_to_process = [url for url in objects_list if pattern.search(url)]

    return object_to_process


def create_query(**kwargs):
    ti = kwargs["ti"]
    files = ti.xcom_pull(task_ids='gcs_objects_to_process')
    files = [f'gs://{BUCKET}/'+ file for file in files]
    INSERT_ROWS_QUERY = (
    f"""
    LOAD DATA INTO {DATASET_NAME}.raw (title string, link string, summary string, authors string, published string, tags string, inserted_at string, file_name string)
    FROM FILES (skip_leading_rows=1, allow_quoted_newlines=True, format = 'CSV', uris = {str(files)});
    """
    )
    return INSERT_ROWS_QUERY


with DAG(
    dag_id="gcs_csv_to_big_query", 
    start_date=datetime(2024, 6, 24), 
    schedule_interval='@daily',
    catchup = True,
    ):



    files = GCSListObjectsOperator(
        task_id="get_all_objects",
        bucket= BUCKET,
        prefix='preprocessed/',
    )

    files_filtered = PythonOperator(           #This task filters the file names to only keep the one uploaded on the execution date
    task_id='gcs_objects_to_process',
    python_callable=gcs_objects_to_load,
    )


    create_query_task = PythonOperator(
    task_id = 'generate_query',
    python_callable = create_query
    )

    insert_query_job = BigQueryInsertJobOperator(
        task_id="insert_query_job",
        configuration={
                "query": {
                    "query": "{{  ti.xcom_pull(task_ids='generate_query')}}",
                    "useLegacySql": False,
                    "priority": "BATCH",
                }
            },
        retries=2,
        retry_exponential_backoff=True
    )


    files >> files_filtered >> create_query_task >> insert_query_job
