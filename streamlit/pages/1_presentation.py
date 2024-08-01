# streamlit_app.py

import streamlit as st
from google.oauth2 import service_account
from google.cloud import bigquery
from wordcloud import WordCloud
import matplotlib.pyplot as plt
import nltk
from nltk.corpus import stopwords
import string
from datetime import datetime
from collections import Counter

# Create API client.
credentials = service_account.Credentials.from_service_account_info(
    st.secrets["gcp_service_account"]
)
client = bigquery.Client(credentials=credentials)

query_articles = f"""
SELECT count(*) FROM `stay-informed-429009.dev_stay_informed.gold`
"""
query_sources = f"""
With sources as (
SELECT source
FROM `stay-informed-429009.dev_stay_informed.gold`
group by source)
select count(*) as nb_source from sources
"""

query_min_day = f"""
select min(date) as min_date from `stay-informed-429009.dev_stay_informed.gold`
"""
query_max_day = f"""
select max(date) as max_date from `stay-informed-429009.dev_stay_informed.gold`
"""

@st.cache_data(ttl=600)
def run_query(query):
    query_job = client.query(query)
    results = query_job.result()
    df = results.to_dataframe()
    return df

st.header("Stay informed presentation")

nb_articles = run_query(query_articles).iloc[0].values[0]
nb_sources = run_query(query_sources).iloc[0].values[0]

min_date = run_query(query_min_day).iloc[0].values[0]
max_date = run_query(query_max_day).iloc[0].values[0]


min_date = datetime.strptime(min_date, '%Y-%m-%d')
max_date = datetime.strptime(max_date, '%y-%m-%d')

delta = max_date - min_date

st.subheader("KPI", divider=True)
col1, col2, col3 = st.columns(3)
col1.metric("Number of articles", nb_articles)
col2.metric("Number of sources", nb_sources)
col3.metric("Articles per day", round(nb_articles/delta.days))

st.subheader("Our sources", divider=True)

image_dict = {
"bbc" :"https://upload.wikimedia.org/wikipedia/commons/6/65/BBC_logo_%281997-2021%29.svg",
"the daily telegraph":"https://upload.wikimedia.org/wikipedia/commons/e/e7/Daily_Telegraph.svg",
"metro":"https://upload.wikimedia.org/wikipedia/commons/3/30/Metro_logo_black_2014.svg",
"l'avenir":"https://play-lh.googleusercontent.com/iZ8KJqxhRoaI6u2jb13nRR4dNVa-RD4KLNC5XbZl8XRfg3wLvPZS_hnu8KFkNuKtig=w240-h480-rw",
"la libre":"https://upload.wikimedia.org/wikipedia/commons/7/78/La_Libre_Belgique_logo.svg",
"le monde":"https://upload.wikimedia.org/wikipedia/commons/4/43/Le_Monde.svg",
"liberation":"https://upload.wikimedia.org/wikipedia/commons/0/0a/Lib%C3%A9ration.svg",
"france info":"https://upload.wikimedia.org/wikipedia/commons/1/18/France_Info_-_2008.svg",
"die welt":"https://upload.wikimedia.org/wikipedia/commons/0/0a/Die_Welt_Logo_2015.png",
"sueddeutsche":"https://upload.wikimedia.org/wikipedia/commons/4/42/S%C3%BCddeutsche_Zeitung_Logo.svg"
}

# Create three columns
col4, col5, col6 = st.columns(3)

# Insert logos in each column
with col4:
    st.markdown("_english speacking_")
    st.image(image_dict["bbc"], width = 150)
    st.image(image_dict["the daily telegraph"], width = 150)
    st.image(image_dict["metro"], width = 150)

with col5:
    st.markdown("_french speacking_")
    st.image(image_dict["le monde"], width = 150)
    st.image(image_dict["liberation"], width = 150)
    st.image(image_dict["la libre"], width = 150)
    st.image(image_dict["l'avenir"], width = 120)
    st.image(image_dict["france info"], width = 120)

with col6:
    st.markdown("_german speacking_")
    st.image(image_dict["die welt"], width = 150)
    st.image(image_dict["sueddeutsche"], width = 150)
