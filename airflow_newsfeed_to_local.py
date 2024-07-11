from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import requests
import os
import json

NEWS_FEEDS = {
    "france": {
        "le monde": "https://www.lemonde.fr/rss/en_continu.xml",
        "liberation": "https://www.liberation.fr/arc/outboundfeeds/rss/?outputtype=xml",
        "france info": "https://www.francetvinfo.fr/titres.rss"
    },
    "belgium":{
        "l'avenir": "https://www.lavenir.net/arc/outboundfeeds/rss/?outputType=xml",
        "la libre": "https://www.lalibre.be/arc/outboundfeeds/rss/?outputType=xml",

    },
    "england": {
        "metro": "https://metro.co.uk/feed/",
        "the daily telegraph": "https://www.telegraph.co.uk/rss.xml",
        "bbc": "http://feeds.bbci.co.uk/news/world/rss.xml"},
    "germany": {
        "sueddeutsche": "https://rss.sueddeutsche.de/alles",
        "die welt": "https://www.welt.de/feeds/latest.rss",
        "rtl": "https://www.rtl.de/rss/feed/news"
    }
}

def save_feed(feed_url: str, file_path: str):
    feed = requests.get(feed_url)
    if feed.status_code == 200:
        with open(file_path, 'w', encoding='utf-8') as f:
            f.write(feed.text)
    return feed.reason

def create_path(country: str, feed_name: str) -> str:
    today = datetime.today()
    path = f'/opt/airflow/data/{country}/{feed_name}/{today.strftime("%Y%m%d-%H%M%S")}.xml'
    os.makedirs(os.path.dirname(path), exist_ok=True)
    return path

def save_all_feeds():
    logs = {}
    log_path = f'/opt/airflow/data/log/{datetime.today().strftime("%Y%m%d-%H%M%S")}.json'
    os.makedirs(os.path.dirname(log_path), exist_ok=True)
    for country, feeds in NEWS_FEEDS.items():
        for feed_name, feed_url in feeds.items():
            file_path = create_path(country, feed_name)
            response = save_feed(feed_url, file_path)
            logs[feed_name] = response
    with open(log_path, 'w', encoding='utf-8') as f:
        json.dump(logs, f)

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 7, 10),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'rss_feed_to_local',
    default_args=default_args,
    description='Fetch RSS feeds and store them locally',
    schedule_interval=timedelta(days=1),
    catchup=False,
) as dag:

    save_feeds_task = PythonOperator(
        task_id='save_all_feeds',
        python_callable = save_all_feeds,
    )

save_feeds_task
