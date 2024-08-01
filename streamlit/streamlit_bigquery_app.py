# streamlit_app.py

import streamlit as st
from google.oauth2 import service_account
from google.cloud import bigquery
from wordcloud import WordCloud
import matplotlib.pyplot as plt
import nltk
from nltk.corpus import stopwords
import datetime

# Create API client.
credentials = service_account.Credentials.from_service_account_info(
    st.secrets["gcp_service_account"]
)
client = bigquery.Client(credentials=credentials)

DATABASE = 'dev_stay_informed'
PROJECT ='stay-informed-429009'

# Perform query.
# Uses st.cache_data to only rerun when the query changes or after 10 min.
@st.cache_data(ttl=600)
def run_query(query):
    query_job = client.query(query)
    rows_raw = query_job.result()
    # Convert to list of dicts. Required for st.cache_data to hash the return value.
    rows = [dict(row) for row in rows_raw]
    return rows


start_date = datetime.date(2024, 6, 24)
end_date = datetime.date.today() - datetime.timedelta(days=2)


st.title("Stay informed")

date = st.date_input("Pick a date:", end_date, start_date, end_date)


summary = run_query(f"""
    SELECT summary 
    FROM `{PROJECT}.{DATABASE}.summaries` 
    WHERE date = '{date}'
""")

rows = run_query(f"""
    WITH row_count AS (
        SELECT COUNT(*) AS row_count
        FROM `{PROJECT}.{DATABASE}.gold` 
    )
                 
    SELECT 
        title
        , link 
    FROM `{PROJECT}.{DATABASE}.gold` 
    LEFT JOIN row_count ON 1=1
    WHERE date = '{date}' 
    ORDER BY FARM_FINGERPRINT(FORMAT('%T', (title || link, "random_seed_2")))
    LIMIT 10
 """)


st.markdown(summary[0]['summary'])
st.markdown("## 🕵️ 10 randoms articles for digging the news:")
for row in rows:
    st.markdown("✍️ " + f"[{row['title']}]({row['link']})")
