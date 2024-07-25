from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.gcs import GCSListObjectsOperator
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.utils.dates import days_ago
from datetime import timedelta, datetime
import csv
import requests
import io
import os
import re
import pandas as pd
import feedparser


def gcs_objects_to_process(**kwargs):
    ti = kwargs["ti"]
    objects_list = ti.xcom_pull(task_ids='list_gcs_objects')
    current_date = kwargs['ds'].replace('-', '')                                #The ds represents the execution date so the dag can be
    pattern = re.compile(rf'raw/.*/{current_date}-\d+\.xml$')
    object_to_process = [url for url in objects_list if pattern.search(url)]

    return object_to_process

def preprocess_RSS_feed_to_tabular(**kwargs):
    ti = kwargs['ti']
    objects_to_preprocess = ti.xcom_pull(task_ids='gcs_objects_to_process')
    gcs_hook = GCSHook()
    bucket_name = BUCKET
    preproccessed_objects = []
    for object_name in objects_to_preprocess:
        file_content = gcs_hook.download(bucket_name, object_name)
        preprossessed_file = parse_xml(xml_content=file_content, source=object_name, execution_date=kwargs['ds'])
        preprossed_object_name = 'preprocessed/' + '/'.join(object_name.split('/')[1:]).rsplit('.', 1)[0] + '.csv'
        gcs_hook.upload(
            bucket_name=bucket_name,
            object_name=preprossed_object_name,
            data=buffer_csv_data(preprossessed_file)
        )
        preproccessed_objects.append(preprossed_object_name)
    print(preproccessed_objects)
    return preproccessed_objects

def parse_xml(**kwargs):
    # Parse the XML file
    feed = feedparser.parse(kwargs['xml_content'])
    parsed_lines = []

    for line in feed.entries:
        line_info = {
            'title': line.get('title', ''),
            'link': line.get('link', ''),
            'summary': line.get('summary', ''),
            'authors': [author.get('name', '') for author in line.get('authors', [])],
            'published': line.get('published', ''),
            'tags': [tag.get('term', '') for tag in line.get('tags', [])]
        }
        line_info['inserted_at'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        line_info['file_name'] = kwargs['source']
        parsed_lines.append(line_info)
    return parsed_lines

def buffer_csv_data(data:list):
  df = pd.DataFrame(data)
  csv_buffer = io.StringIO()
  df.to_csv(csv_buffer, index=False, encoding = 'utf-8')
  csv_content = csv_buffer.getvalue()
  return csv_content

dag = DAG(
    'GCS_xml_to_GCS_csv',
    description='Process RSS feeds from CSV file',
    start_date=datetime(2024, 6, 24),
    schedule_interval='@daily',
    catchup = True
)

with dag:

    BUCKET = os.getenv('BUCKET_NAME')

    list_gcs_objects_task = GCSListObjectsOperator(         #This task fetches all file names inside the raw folder
    task_id='list_gcs_objects',
    bucket = BUCKET,
    gcp_conn_id='google_cloud_default',
    prefix = f'raw/',
    dag=dag,
    )

    gcs_objects_to_process_task = PythonOperator(           #This task filters the file names to only keep the one uploaded on the execution date
        task_id='gcs_objects_to_process',
        python_callable=gcs_objects_to_process,
    )

    preprocess_task = PythonOperator(
    task_id='preprocess_rss_feeds',
    python_callable=preprocess_RSS_feed_to_tabular,
    )

    list_gcs_objects_task >> gcs_objects_to_process_task >> preprocess_task
