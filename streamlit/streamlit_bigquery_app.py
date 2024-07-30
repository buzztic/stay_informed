# streamlit_app.py

import streamlit as st
from google.oauth2 import service_account
from google.cloud import bigquery

import datetime

# Create API client.
credentials = service_account.Credentials.from_service_account_info(
    st.secrets["gcp_service_account"]
)
client = bigquery.Client(credentials=credentials)



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

date = st.date_input("From which date do want somes news ?", end_date, start_date, end_date)

rows = run_query(f"SELECT title, link FROM `stay-informed-429009.dev_stay_informed.gold` WHERE date = '{date}' LIMIT 10 ")

# Print results.

st.write("Somes articles from that days")
for row in rows:
    st.markdown("✍️ " + f"[{row['title']}]({row['link']})")
