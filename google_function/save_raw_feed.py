import requests
from datetime import datetime
import os
import json
from google.cloud import storage

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

BUCKET_NAME = os.getenv('BUCKET_NAME')

def upload_to_gs(bucket_name, blob_text, destination_blob_name):
    """Uploads a file to the bucket."""
    storage_client = storage.Client()
    bucket = storage_client.get_bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)

    blob.upload_from_string(blob_text)

def save_feed(feed_url: str, file_path:str):
    feed = requests.get(feed_url)
    if feed.status_code == 200:
        upload_to_gs(BUCKET_NAME, feed.text, file_path)
    return feed.reason

def create_path(country: str, feed_name: str) -> str:
    today = datetime.today()
    path = f'{country}/{feed_name}/{today.strftime("%Y%m%d-%H%M%S")}.xml'
    os.makedirs(os.path.dirname(path), exist_ok=True)
    return path

def save_all_feeds_to_gs(data, context): 
    # data, context are required in google function 
    logs = {}
    log_path = f'log/{datetime.today().strftime("%Y%m%d-%H%M%S")}.json'
    for country, feeds in NEWS_FEEDS.items():
        for feed_name, feed_url in feeds.items():
            response = save_feed(feed_url, create_path(country, feed_name))
            logs[feed_name] = response
    upload_to_gs(BUCKET_NAME, json.dumps(logs),log_path)

