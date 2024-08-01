# streamlit_app.py

import streamlit as st
from google.oauth2 import service_account
from google.cloud import bigquery
from wordcloud import WordCloud
import matplotlib.pyplot as plt
import nltk
from nltk.corpus import stopwords
import string
import datetime
from collections import Counter

# Create API client.
credentials = service_account.Credentials.from_service_account_info(
    st.secrets["gcp_service_account"]
)
client = bigquery.Client(credentials=credentials)

# Download stop words for English, German, and French -> Should be put into bigquery
nltk.download('stopwords')
stop_words_en = set(stopwords.words('english'))
stop_words_de = set(stopwords.words('german'))
stop_words_fr = set(stopwords.words('french'))


all_stop_words = stop_words_en.union(stop_words_de).union(stop_words_fr).union(set(list(string.punctuation)))
all_stop_words.update(["après","un","say","says","s",'thousands',"«","»"])


start_date = datetime.date(2024, 6, 24)
end_date = datetime.date.today() - datetime.timedelta(days=2)

st.title("Daily Trends")
date = st.date_input("Select the date ", end_date, start_date, end_date)

source_choice = ["bbc","metro", "die welt", "l'avenir","la libre","le monde","liberation","france info","sueddeutsche", "the daily telegraph"]

options = st.multiselect(
    "Select the sources",
    source_choice,
    ["bbc"],
)

sources_list = ", ".join([f"'{source}'" for source in options])

query_multi = f"""
SELECT
    ARRAY_TO_STRING(ARRAY_AGG(title), ' ') AS combined_summary
FROM `stay-informed-429009.dev_stay_informed.gold`
WHERE date = '{date}'
    AND source IN ({sources_list})
"""

@st.cache_data(ttl=600)
def run_query(query):
    query_job = client.query(query)
    results = query_job.result()
    df = results.to_dataframe()
    return df

if options :
    # Convert to DataFrame
    df = run_query(query_multi)

    combined_text = df['combined_summary'].iloc[0]

    words = [word.lower() for word in combined_text.split() if word.lower() not in all_stop_words]

    text_for_wordcloud = ','.join(words).replace("d’","").replace("l’","")


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

    word_count = Counter(words).most_common(10)

    st.header("Here are the most mentioned topic")
    st.markdown("🥇" + f" _{word_count[0][0]}_"+f" mentioned {word_count[0][1]} times")
    st.markdown("🥈" + f" _{word_count[1][0]}_"+f" mentioned {word_count[1][1]} times")
    st.markdown("🥉" + f" _{word_count[2][0]}_"+f" mentioned {word_count[2][1]} times")

    st.header("other trending topics")
    for element in word_count[3:]:
        st.markdown(f"{element[0]}")
