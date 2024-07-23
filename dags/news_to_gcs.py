from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from datetime import  datetime
import csv
import requests
import io
import os

BUCKET = os.getenv('BUCKET_NAME')

def get_csv_data(**kwargs):
    gcs_hook = GCSHook()
    object_name = 'rss_feeds.csv'

    file_content = gcs_hook.download(BUCKET, object_name)
    csv_data = csv.DictReader(io.StringIO(file_content.decode('utf-8')))

    return list(csv_data)

def process_rss_feeds(**kwargs):
    ti = kwargs['ti']
    csv_data = ti.xcom_pull(task_ids='get_csv_data')
    gcs_hook = GCSHook()
    execution_date = datetime.today.strftime("%Y%m%d-%H%M%S")
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
                bucket_name=BUCKET,
                object_name=object_name,
                data=xml_content,
                mime_type='application/xml'
            )
            raw_objects.append(object_name)
            print(f"Successfully uploaded {object_name}")
        else:
            print(f"Failed to fetch RSS feed from {url}")


dag = DAG(
    'rss_feed_processor',
    description='Process RSS feeds from CSV file',
    start_date=datetime(2024, 7, 18),
    schedule_interval='@daily',
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

    get_csv_task >> process_rss_task
