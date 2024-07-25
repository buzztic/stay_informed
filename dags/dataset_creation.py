from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryCreateEmptyDatasetOperator, 
    BigQueryGetDatasetOperator, 
    BigQueryExecuteQueryOperator
)
from airflow import DAG
from datetime import datetime
import os

with DAG(
    'dataset_creation',
    description='Check if dataset exist and create if not',
    schedule_interval='@daily',
    start_date=datetime(2024, 7, 18),
) as dag:


    DATASET_NAME  = os.getenv('BQ_DATASET_NAME')
    
    get_dataset = BigQueryGetDatasetOperator(
        task_id="get-dataset", 
        dataset_id=DATASET_NAME
    )

    
    create_dataset = BigQueryCreateEmptyDatasetOperator(
        dataset_id=DATASET_NAME,
        task_id='create-dataset',
        trigger_rule='one_failed'
    )

    create_raw_table = BigQueryExecuteQueryOperator(
        task_id="create_raw_table",
        sql=f"""
            CREATE TABLE IF NOT EXISTS {DATASET_NAME}.raw (
            title STRING,
            link STRING,
            summary STRING,
            authors STRING,
            published STRING,
            tags STRING,
            inserted_at STRING,
            file_name STRING,
        )""",
        use_legacy_sql=False,
    )

    get_dataset >> create_dataset
    get_dataset >> create_raw_table
