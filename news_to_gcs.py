from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.utils.dates import days_ago
from datetime import timedelta, datetime
import csv
import requests
import io
import pandas as pd
import feedparser

def get_csv_data(**kwargs):
    gcs_hook = GCSHook(gcp_conn_id='google_cloud_connection')
    bucket_name = 'correct_bucket_name'
    object_name = 'rss_feeds.csv'

    file_content = gcs_hook.download(bucket_name, object_name)
    csv_data = csv.DictReader(io.StringIO(file_content.decode('utf-8')))

    return list(csv_data)

def process_rss_feeds(**kwargs):
    ti = kwargs['ti']
    csv_data = ti.xcom_pull(task_ids='get_csv_data')
    gcs_hook = GCSHook(gcp_conn_id='google_cloud_connection') #This connection needs to be defined
    output_bucket = 'correct_bucket_name'
    execution_date = kwargs['execution_date'].strftime('%Y-%m-%d')
    raw_objects = []
    for row in csv_data:
        country = row['country']
        source = row['source']
        url = row['url']

        response = requests.get(url)
        if response.status_code == 200:
            xml_content = response.content

            # Create a proper object name
            object_name = f"raw/{country}/{source}/{execution_date}.xml"

            # Upload the content to GCS
            gcs_hook.upload(
                bucket_name=output_bucket,
                object_name=object_name,
                data=xml_content,
                mime_type='application/xml'
            )
            raw_objects.append(object_name)
            print(f"Successfully uploaded {object_name}")
        else:
            print(f"Failed to fetch RSS feed from {url}")
    return raw_objects

def preprocess_RSS_feed_to_tabular(**kwargs):
    ti = kwargs['ti']
    objects_to_preprocess = ti.xcom_pull(task_ids='process_rss_feeds')
    gcs_hook = GCSHook(gcp_conn_id='google_cloud_connection')
    bucket_name = 'correct_bucket_name'
    preproccessed_objects = []
    for object_name in objects_to_preprocess:
        file_content = gcs_hook.download(bucket_name, object_name)
        preprossessed_file = parse_xml(xml_content=file_content, source=object_name, execution_date=kwargs['execution_date'])
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
        line_info['load_date'] = kwargs['execution_date'].strftime('%Y-%m-%d')
        line_info['source'] = kwargs['source']
        parsed_lines.append(line_info)
    return parsed_lines


def buffer_csv_data(data:list):
  df = pd.DataFrame(data)
  csv_buffer = io.StringIO()
  df.to_csv(csv_buffer, index=False)
  csv_content = csv_buffer.getvalue()
  return csv_content

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 7, 12),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'rss_feed_processor',
    default_args=default_args,
    description='Process RSS feeds from CSV file',
    schedule_interval=timedelta(days=1),
    catchup = False
)
with dag:
    get_csv_task = PythonOperator(
        task_id='get_csv_data',
        python_callable=get_csv_data,
    )

    process_rss_task = PythonOperator(
        task_id='process_rss_feeds',
        python_callable=process_rss_feeds,
    )

    preprocess_task = PythonOperator(
        task_id='preprocess_rss_feeds',
        python_callable=preprocess_RSS_feed_to_tabular,
    )

    get_csv_task >> process_rss_task >> preprocess_task
