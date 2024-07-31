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

st.write("Word Cloud")

source_choice = ["bbc","metro", "die welt", "l'avenir","la libre","le monde","liberation","france info","sueddeutsche", "the daily telegraph"]

source = st.selectbox(
    "select the source from which you want to see the most frequent subject",
    source_choice,
    placeholder = "bbc"
)

# Download stop words for English, German, and French -> Should be put into bigquery
nltk.download('stopwords')
stop_words_en = set(stopwords.words('english'))
stop_words_de = set(stopwords.words('german'))
stop_words_fr = set(stopwords.words('french'))

all_stop_words = stop_words_en.union(stop_words_de).union(stop_words_fr)

query = f"""
SELECT
  ARRAY_TO_STRING(ARRAY_AGG(title), ' ') AS combined_summary
FROM `stay-informed-429009.dev_stay_informed.gold`
WHERE date = '{date}'
  and source = '{source}'
LIMIT 10
"""

# Run the query and fetch results
query_job = client.query(query)
results = query_job.result()

# Convert to DataFrame
df = results.to_dataframe()

combined_text = df['combined_summary'].iloc[0]

words = [word.lower() for word in combined_text.split() if word.lower() not in all_stop_words]

text_for_wordcloud = ','.join(words)


# Create and generate a word cloud image:
wordcloud = WordCloud(width=800, height=400, background_color='white').generate(text_for_wordcloud)


# Create a figure and axis
fig, ax = plt.subplots(figsize=(10, 5))

# Display the generated image:
ax.imshow(wordcloud, interpolation='bilinear')
ax.axis("off")

# Instead of plt.show(), use st.pyplot(fig)
st.pyplot(fig)
# st.write("Raw text is : ")
# st.write(text_for_wordcloud)
# st.write("stop_word are : ")
# st.write(stop_words_fr)
