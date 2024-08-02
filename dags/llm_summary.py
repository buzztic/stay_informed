from datetime import datetime
import google.generativeai as genai

from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import  (
    BigQueryGetDataOperator,
    BigQueryExecuteQueryOperator,
    BigQueryInsertJobOperator
)
from airflow.operators.python import PythonOperator
from airflow.sensors.external_task import ExternalTaskSensor
from google.cloud import bigquery
import logging
import os

DATASET_NAME  = os.getenv('BQ_DATASET_NAME')


def escape_text_for_bigquery(text):
    text = text.replace('\\', '\\\\')
    text = text.replace("'", "\\'")
    text = text.replace('\n', '\\n')
    text = text.replace('"', '\\"')
    return text


def create_insert_query(**kwargs):
    summary = kwargs['ti'].xcom_pull(task_ids='create_summary', key='return_value')
    if not summary:
        raise ValueError("No summary to insert into BigQuery.")
    INSERT_ROWS_QUERY = (
        f"INSERT {DATASET_NAME}.summaries VALUES "
        f"('{kwargs['ds']}', '{escape_text_for_bigquery(summary)}')"
    )
    return INSERT_ROWS_QUERY



def create_summary(ti):
    titles = ti.xcom_pull(task_ids='fetch_titles', key='return_value')
    titles_list = [row[0] for row in titles]
    prompt = 'You are a journalist who summarize this news-titles in less than 300 words and give only the main news classified in 3 categories: international, sport and others. no more than 4 news per category. Here the titles: '
    model = genai.GenerativeModel('gemini-1.5-flash')
    response = model.generate_content(prompt + str(titles_list))
    logging.info("Titles: %s", response.text)
    return response.text


with DAG(
    dag_id="llm_summary",
    start_date=datetime(2024, 6, 24),
    schedule_interval='@daily',
    catchup = True,
    ):

    wait_for_transform_raw_to_silver = ExternalTaskSensor(
        task_id='wait_for_transform_raw_to_silver',
        external_dag_id='transform_raw_to_silver',
        external_task_id='run_dbt_docker'
    )

    create_summary_table = BigQueryExecuteQueryOperator(
        task_id="create_summary_table",
        sql=f"""
            CREATE TABLE IF NOT EXISTS {DATASET_NAME}.summaries (
            date STRING,
            summary STRING,

        )""",
        use_legacy_sql=False,
    )

    create_summary_task = PythonOperator(
        task_id='create_summary',
        python_callable=create_summary,
        provide_context=True,
    )

    TEMP_TABLE = f"{DATASET_NAME}.temp_table_for_titles"
    SQL_QUERY = f"SELECT distinct title FROM `stay-informed-429009.{DATASET_NAME}.gold` " + "WHERE date='{{ ds }}'"
    execute_query = BigQueryExecuteQueryOperator(
        task_id='execute_query',
        sql=SQL_QUERY,
        destination_dataset_table=TEMP_TABLE,
        write_disposition='WRITE_TRUNCATE',
        use_legacy_sql=False,
    )

    fetch_titles = BigQueryGetDataOperator(
        task_id='fetch_titles',
        dataset_id=DATASET_NAME,
        table_id='temp_table_for_titles',
    )

    create_insert_query_task = PythonOperator(
        task_id = 'generate_insert_query',
        python_callable = create_insert_query
    )

    insert_query_job = BigQueryInsertJobOperator(
        task_id="insert_query_job",
        configuration={
                "query": {
                    "query": "{{  ti.xcom_pull(task_ids='generate_insert_query')}}",
                    "useLegacySql": False,
                }
            },
    )
    wait_for_transform_raw_to_silver >> execute_query
    execute_query >> fetch_titles >> create_summary_task >> create_summary_table
    create_summary_table >> create_insert_query_task >> insert_query_job
