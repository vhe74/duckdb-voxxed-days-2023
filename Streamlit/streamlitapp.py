import streamlit as st
import pandas as pd
import duckdb
from dotenv import load_dotenv
import os

## setup
# env
load_dotenv(dotenv_path="/root/work/vhe/env.txt")
ACCESS_KEY_ID=os.getenv("ACCESS_KEY_ID")
SECRET_ACCESS_KEY=os.getenv("SECRET_ACCESS_KEY")

# duckdb
duckdb.install_extension("httpfs")
duckdb.load_extension("httpfs")
duckdb.sql(f"SET s3_region='fr-par';")
duckdb.sql(f"SET s3_access_key_id='{ACCESS_KEY_ID}';")
duckdb.sql(f"SET s3_secret_access_key='{SECRET_ACCESS_KEY}';")
duckdb.sql(f"SET s3_endpoint='s3.fr-par.scw.cloud';")

S3_BUCKET = os.environ.get("S3_BUCKET", "crypto-histo")
PARQUET_FOLDER = os.environ.get("PARQUET_FOLDER", "parquet")


# get top 10 symbol
top10Symbol = duckdb.sql(f"""
SELECT symbol, max(max) max
FROM read_parquet('s3://{S3_BUCKET}/crypto/index.parquet')
GROUP BY symbol
ORDER BY max DESC 
LIMIT 10
""").to_df()

# get the symbol list

allSymbol = duckdb.sql(f"""
SELECT symbol,
FROM read_parquet('s3://{S3_BUCKET}/crypto/index.parquet')
GROUP BY symbol
ORDER BY max(max) DESC 
LIMIT 50
""").to_df()

## UI
#st.set_page_config(layout="wide")
placeholder = st.empty()
placeholder.title("Crypto assets")


col1, col2 = st.columns(2)   
with col1 : 
    st.header("Top 10 values")
    st.dataframe(top10Symbol)

allValues = duckdb.sql(f""" 
      SELECT * 
      FROM read_parquet('s3://{S3_BUCKET}/crypto/index.parquet') 
      WHERE d > '2021-12-31' AND d < '2023-01-01'
    """)

with col2 : 
    st.header("Pick two")
    option1 = st.selectbox('First symbol : ', allSymbol, key='opt1')
    option2 = st.selectbox('Second symbol : ', allSymbol, key='opt2')

    values = duckdb.sql(f"""
      SELECT v1.d as date, max(v1.max) {option1}, max(v2.max) {option2},
      FROM allValues v1
      JOIN allValues v2 on v1.d = v2.d
      WHERE v1.symbol = '{option1}' AND v2.symbol = '{option2}'
      GROUP BY date
      ORDER BY date ASC 
    """).to_df()

    #st.dataframe(values)
    st.line_chart(values, x='date')


